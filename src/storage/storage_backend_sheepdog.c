/*
 * storage_backend_sheepdog.c: storage backend for Sheepdog handling
 *
 * Copyright (C) 2012 Wido den Hollander
 * Copyright (C) 2012 Frank Spijkerman
 * Copyright (C) 2012 Sebastian Wiedenroth
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library;  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Author: Wido den Hollander <wido@widodh.nl>
 *         Frank Spijkerman <frank.spijkerman@avira.com>
 *         Sebastian Wiedenroth <sebastian.wiedenroth@skylime.net>
 */

#include <config.h>

#include "virterror_internal.h"
#include "storage_backend_sheepdog.h"
#include "storage_conf.h"
#include "util/command.h"
#include "util.h"
#include "memory.h"
#include "logging.h"

#define VIR_FROM_THIS VIR_FROM_STORAGE

static int virStorageBackendSheepdogRefreshVol(virConnectPtr conn,
                                               virStoragePoolObjPtr pool,
                                               virStorageVolDefPtr vol);

void virStorageBackendSheepdogAddHostArg(virCommandPtr cmd,
                                         virStoragePoolObjPtr pool);

int
virStorageBackendSheepdogParseNodeInfo(virStoragePoolDefPtr pool,
                                       char *output)
{
    /* fields:
     * node id/total, size, used, use%, [total vdi size]
     *
     * example output:
     * 0 15245667872 117571104 0%
     * Total 15245667872 117571104 0% 20972341
     */

    const char *p, *next;

    pool->allocation = pool->capacity = pool->available = 0;

    p = output;
    do {
        char *end;

        if ((next = strchr(p, '\n')))
            ++next;
        else
            return -1;

        if (!STRPREFIX(p, "Total "))
            continue;

        p = p + 6;

        if (virStrToLong_ull(p, &end, 10, &pool->capacity) < 0)
            return -1;

        if ((p = end + 1) > next)
            return -1;

        if (virStrToLong_ull(p, &end, 10, &pool->allocation) < 0)
            return -1;

        pool->available = pool->capacity - pool->allocation;
        return 0;

    } while ((p = next));

    return -1;
}

void
virStorageBackendSheepdogAddHostArg(virCommandPtr cmd,
                                    virStoragePoolObjPtr pool)
{
    const char *address = "localhost";
    int port = 7000;
    if (pool->def->source.nhost > 0) {
        if (pool->def->source.hosts[0].name != NULL) {
            address = pool->def->source.hosts[0].name;
        }
        if (pool->def->source.hosts[0].port) {
            port = pool->def->source.hosts[0].port;
        }
    }
    virCommandAddArg(cmd, "-a");
    virCommandAddArgFormat(cmd, "%s", address);
    virCommandAddArg(cmd, "-p");
    virCommandAddArgFormat(cmd, "%d", port);
}


static int
virStorageBackendSheepdogRefreshPool(virConnectPtr conn ATTRIBUTE_UNUSED,
                                     virStoragePoolObjPtr pool)
{
    int ret;
    char *output = NULL;
    virCommandPtr cmd;

    cmd = virCommandNewArgList(COLLIE, "node", "info", "-r", NULL);
    virStorageBackendSheepdogAddHostArg(cmd, pool);
    virCommandSetOutputBuffer(cmd, &output);
    ret = virCommandRun(cmd, NULL);
    if (ret == 0)
        ret = virStorageBackendSheepdogParseNodeInfo(pool->def, output);

    if (ret != 0) goto cleanup;

    virCommandFree(cmd);
    VIR_FREE(output);

    cmd = virCommandNewArgList(COLLIE, "vdi","list","-r", NULL);
    virCommandSetOutputBuffer(cmd, &output);
    ret = virCommandRun(cmd, NULL);
    if (ret == 0)
        ret = virStorageBackendSheepdogParseVdiList(pool, output);

cleanup:
    virCommandFree(cmd);
    VIR_FREE(output);
    return ret;

}


static int
virStorageBackendSheepdogDeleteVol(virConnectPtr conn ATTRIBUTE_UNUSED,
                                   virStoragePoolObjPtr pool,
                                   virStorageVolDefPtr vol,
                                   unsigned int flags)
{

    virCheckFlags(0, -1);

    virCommandPtr cmd = virCommandNewArgList(COLLIE, "vdi", "delete", vol->name, NULL);
    virStorageBackendSheepdogAddHostArg(cmd, pool);
    int ret = virCommandRun(cmd, NULL);

    virCommandFree(cmd);
    return ret;
}


static int
virStorageBackendSheepdogCreateVol(virConnectPtr conn ATTRIBUTE_UNUSED,
                                   virStoragePoolObjPtr pool,
                                   virStorageVolDefPtr vol)
{

    int ret;

    if (vol->target.encryption != NULL) {
        virReportError(VIR_ERR_CONFIG_UNSUPPORTED, "%s",
                       _("Sheepdog does not support encrypted volumes"));
        return -1;
    }

    virCommandPtr cmd = virCommandNewArgList(COLLIE, "vdi", "create", vol->name, NULL);
    virCommandAddArgFormat(cmd, "%llu", vol->capacity);
    virStorageBackendSheepdogAddHostArg(cmd, pool);
    ret = virCommandRun(cmd, NULL);

    virStorageBackendSheepdogRefreshVol(conn, pool, vol);

    virCommandFree(cmd);
    return ret;
}

int
virStorageBackendSheepdogParseVdiList(virStoragePoolObjPtr pool,
                                      char *output)
{
    /* fields:
     * type,name,id,size,used,shared,creation time,vdi id, [ta]
     *
     * example output:
     * s 650f4363-dd7b-4aba-a954-7d6e1ab0ba51 1 2097152000 0 2088763392 1343921684 5fda1
     * = 650f4363-dd7b-4aba-a954-7d6e1ab0ba51 2 2097152000 381681664 1707081728 1343921685 5fda2
     * = dd5089ac-0677-4463-8981-9b7f4c81ed75 1 10485760 8388608 0 1343909537 1c329d
     * = db0ecced-3d23-4a50-a282-df10e6324e5d 1 10485760 8388608 0 1343921037 1fea11
     * = eb1e30fb-d373-4ba9-aede-5b0445202111 1 10485760 8388608 0 1343921382 47b19c
     * = acf1d6f0-e132-4040-aadf-6d53b58f2fd8 1 10485760 8388608 0 1343909627 55f030
     * s 79d9030f-8409-40b9-8b99-f90c966c244d 1 8589934592 0 2172649472 1344337550 62751b
     */

    int id;
    const char *p, *next, *name;
    virStorageVolDefPtr vol = NULL;
    p = output;

    do {
        char *end;

        if ((next = strchr(p, '\n')))
            ++next;

        /* ignore snapshots */
        if (*p != '=')
            continue;

        /* skip space */
        if (p + 2 < next) {
            p += 2;
        } else {
            return -1;
        }

        /* lookup name */
        name = p;
        while (*p != '\0' && *p != ' ') {
            if (*p == '\\')
                ++p;
            ++p;
        }

        if (VIR_ALLOC(vol) < 0) goto no_memory;
        if ((vol->name = strndup(name, p-name)) == NULL) goto cleanup;
        vol->type = VIR_STORAGE_VOL_NETWORK;

        VIR_FREE(vol->target.path);
        if (virAsprintf(&vol->target.path, "%s",
                        vol->name) == -1) {
             virReportOOMError();
             goto cleanup;
        }

        VIR_FREE(vol->key);
        if (virAsprintf(&vol->key, "%s/%s",
	                pool->def->source.name, vol->name) == -1) {
            virReportOOMError();
            goto cleanup;
        }

        if (virStrToLong_i(p, &end, 10, &id) < 0)
            return -1;

        p = end + 1;

        if (virStrToLong_ull(p, &end, 10, &vol->capacity) < 0)
            return -1;

        p = end + 1;

        if (virStrToLong_ull(p, &end, 10, &vol->allocation) < 0)
            return -1;

        if (VIR_REALLOC_N(pool->volumes.objs,
                          pool->volumes.count+1) < 0)
            goto no_memory;
        pool->volumes.objs[pool->volumes.count++] = vol;
        vol = NULL;

    } while ((p = next));

    return 0;

no_memory:
    virReportOOMError();
    /* fallthrough */

cleanup:
    virStorageVolDefFree(vol);
    virStoragePoolObjClearVols(pool);
    return -1;

}

int
virStorageBackendSheepdogParseVdi(virStorageVolDefPtr vol,
                                      char *output)
{
    /* fields:
     * current/clone/snapshot, name, id, size, used, shared, creation time, vdi id, [tag]
     *
     * example output:
     * s test 1 10 0 0 1336556634 7c2b25
     * s test 2 10 0 0 1336557203 7c2b26
     * = test 3 10 0 0 1336557216 7c2b27
     */

    int id;
    const char *p, *next;

    vol->allocation = vol->capacity = 0;

    p = output;
    do {
        char *end;

        if ((next = strchr(p, '\n')))
            ++next;

        /* ignore snapshots */
        if (*p != '=')
            continue;

        /* skip space */
        if (p + 2 < next) {
            p += 2;
        } else {
            return -1;
        }

        /* skip name */
        while (*p != '\0' && *p != ' ') {
            if (*p == '\\')
                ++p;
            ++p;
        }

        if (virStrToLong_i(p, &end, 10, &id) < 0)
            return -1;

        p = end + 1;

        if (virStrToLong_ull(p, &end, 10, &vol->capacity) < 0)
            return -1;

        p = end + 1;

        if (virStrToLong_ull(p, &end, 10, &vol->allocation) < 0)
            return -1;

        return 0;
    } while ((p = next));

    return -1;
}

static int
virStorageBackendSheepdogRefreshVol(virConnectPtr conn ATTRIBUTE_UNUSED,
                                    virStoragePoolObjPtr pool,
                                    virStorageVolDefPtr vol)
{
    int ret;
    char *output = NULL;

    virCommandPtr cmd = virCommandNewArgList(COLLIE, "vdi", "list", vol->name, "-r", NULL);
    virStorageBackendSheepdogAddHostArg(cmd, pool);
    virCommandSetOutputBuffer(cmd, &output);
    ret = virCommandRun(cmd, NULL);

    if (ret < 0)
        goto cleanup;

    if ((ret = virStorageBackendSheepdogParseVdi(vol, output)) < 0)
        goto cleanup;

    vol->type = VIR_STORAGE_VOL_NETWORK;

    VIR_FREE(vol->key);
    if (virAsprintf(&vol->key, "%s/%s",
                    pool->def->source.name, vol->name) == -1) {
        virReportOOMError();
        goto cleanup;
    }

    VIR_FREE(vol->target.path);
    if (virAsprintf(&vol->target.path, "%s", vol->name) == -1) {
        virReportOOMError();
        goto cleanup;
    }

cleanup:
    virCommandFree(cmd);
    return ret;
}


static int
virStorageBackendSheepdogResizeVol(virConnectPtr conn ATTRIBUTE_UNUSED,
                                   virStoragePoolObjPtr pool,
                                   virStorageVolDefPtr vol,
                                   unsigned long long capacity,
                                   unsigned int flags)
{

    virCheckFlags(0, -1);

    virCommandPtr cmd = virCommandNewArgList(COLLIE, "vdi", "resize", vol->name, NULL);
    virCommandAddArgFormat(cmd, "%llu", capacity);
    virStorageBackendSheepdogAddHostArg(cmd, pool);
    int ret = virCommandRun(cmd, NULL);

    virCommandFree(cmd);
    return ret;

}



virStorageBackend virStorageBackendSheepdog = {
    .type = VIR_STORAGE_POOL_SHEEPDOG,

    .refreshPool = virStorageBackendSheepdogRefreshPool,
    .createVol = virStorageBackendSheepdogCreateVol,
    .refreshVol = virStorageBackendSheepdogRefreshVol,
    .deleteVol = virStorageBackendSheepdogDeleteVol,
    .resizeVol = virStorageBackendSheepdogResizeVol,
};
