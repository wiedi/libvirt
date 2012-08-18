/*
 * virsh.h: a shell to exercise the libvirt API
 *
 * Copyright (C) 2005, 2007-2012 Red Hat, Inc.
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
 * Daniel Veillard <veillard@redhat.com>
 * Karel Zak <kzak@redhat.com>
 * Daniel P. Berrange <berrange@redhat.com>
 */

#ifndef VIRSH_H
#define VIRSH_H

# include <stdio.h>
# include <stdlib.h>
# include <string.h>
# include <stdarg.h>
# include <unistd.h>
# include <sys/stat.h>
# include <inttypes.h>

# include "internal.h"
# include "virterror_internal.h"
# include "threads.h"
# include "virnetdevbandwidth.h"

# define VIRSH_MAX_XML_FILE 10*1024*1024

# define VSH_PROMPT_RW    "virsh # "
# define VSH_PROMPT_RO    "virsh > "

# define VIR_FROM_THIS VIR_FROM_NONE

# define GETTIMEOFDAY(T) gettimeofday(T, NULL)
# define DIFF_MSEC(T, U) \
        ((((int) ((T)->tv_sec - (U)->tv_sec)) * 1000000.0 + \
          ((int) ((T)->tv_usec - (U)->tv_usec))) / 1000.0)

/* Default escape char Ctrl-] as per telnet */
# define CTRL_CLOSE_BRACKET "^]"

/**
 * The log configuration
 */
# define MSG_BUFFER    4096
# define SIGN_NAME     "virsh"
# define DIR_MODE      (S_IWUSR | S_IRUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)  /* 0755 */
# define FILE_MODE     (S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH)                                /* 0644 */
# define LOCK_MODE     (S_IWUSR | S_IRUSR)                                                    /* 0600 */
# define LVL_DEBUG     "DEBUG"
# define LVL_INFO      "INFO"
# define LVL_NOTICE    "NOTICE"
# define LVL_WARNING   "WARNING"
# define LVL_ERROR     "ERROR"

/**
 * vshErrorLevel:
 *
 * Indicates the level of a log message
 */
typedef enum {
    VSH_ERR_DEBUG = 0,
    VSH_ERR_INFO,
    VSH_ERR_NOTICE,
    VSH_ERR_WARNING,
    VSH_ERR_ERROR
} vshErrorLevel;

# define VSH_DEBUG_DEFAULT VSH_ERR_ERROR

/*
 * virsh command line grammar:
 *
 *    command_line    =     <command>\n | <command>; <command>; ...
 *
 *    command         =    <keyword> <option> [--] <data>
 *
 *    option          =     <bool_option> | <int_option> | <string_option>
 *    data            =     <string>
 *
 *    bool_option     =     --optionname
 *    int_option      =     --optionname <number> | --optionname=<number>
 *    string_option   =     --optionname <string> | --optionname=<string>
 *
 *    keyword         =     [a-zA-Z][a-zA-Z-]*
 *    number          =     [0-9]+
 *    string          =     ('[^']*'|"([^\\"]|\\.)*"|([^ \t\n\\'"]|\\.))+
 *
 */

/*
 * vshCmdOptType - command option type
 */
typedef enum {
    VSH_OT_BOOL,     /* optional boolean option */
    VSH_OT_STRING,   /* optional string option */
    VSH_OT_INT,      /* optional or mandatory int option */
    VSH_OT_DATA,     /* string data (as non-option) */
    VSH_OT_ARGV,     /* remaining arguments */
    VSH_OT_ALIAS,    /* alternate spelling for a later argument */
} vshCmdOptType;

/*
 * Command group types
 */
# define VSH_CMD_GRP_DOM_MANAGEMENT   "Domain Management"
# define VSH_CMD_GRP_DOM_MONITORING   "Domain Monitoring"
# define VSH_CMD_GRP_STORAGE_POOL     "Storage Pool"
# define VSH_CMD_GRP_STORAGE_VOL      "Storage Volume"
# define VSH_CMD_GRP_NETWORK          "Networking"
# define VSH_CMD_GRP_NODEDEV          "Node Device"
# define VSH_CMD_GRP_IFACE            "Interface"
# define VSH_CMD_GRP_NWFILTER         "Network Filter"
# define VSH_CMD_GRP_SECRET           "Secret"
# define VSH_CMD_GRP_SNAPSHOT         "Snapshot"
# define VSH_CMD_GRP_HOST_AND_HV      "Host and Hypervisor"
# define VSH_CMD_GRP_VIRSH            "Virsh itself"

/*
 * Command Option Flags
 */
enum {
    VSH_OFLAG_NONE     = 0,        /* without flags */
    VSH_OFLAG_REQ      = (1 << 0), /* option required */
    VSH_OFLAG_EMPTY_OK = (1 << 1), /* empty string option allowed */
    VSH_OFLAG_REQ_OPT  = (1 << 2), /* --optionname required */
};

/* dummy */
typedef struct __vshControl vshControl;
typedef struct __vshCmd vshCmd;

/*
 * vshCmdInfo -- name/value pair for information about command
 *
 * Commands should have at least the following names:
 * "name" - command name
 * "desc" - description of command, or empty string
 */
typedef struct {
    const char *name;           /* name of information, or NULL for list end */
    const char *data;           /* non-NULL information */
} vshCmdInfo;

/*
 * vshCmdOptDef - command option definition
 */
typedef struct {
    const char *name;           /* the name of option, or NULL for list end */
    vshCmdOptType type;         /* option type */
    unsigned int flags;         /* flags */
    const char *help;           /* non-NULL help string; or for VSH_OT_ALIAS
                                 * the name of a later public option */
} vshCmdOptDef;

/*
 * vshCmdOpt - command options
 *
 * After parsing a command, all arguments to the command have been
 * collected into a list of these objects.
 */
typedef struct vshCmdOpt {
    const vshCmdOptDef *def;    /* non-NULL pointer to option definition */
    char *data;                 /* allocated data, or NULL for bool option */
    struct vshCmdOpt *next;
} vshCmdOpt;

/*
 * Command Usage Flags
 */
enum {
    VSH_CMD_FLAG_NOCONNECT = (1 << 0),  /* no prior connection needed */
    VSH_CMD_FLAG_ALIAS     = (1 << 1),  /* command is an alias */
};

/*
 * vshCmdDef - command definition
 */
typedef struct {
    const char *name;           /* name of command, or NULL for list end */
    bool (*handler) (vshControl *, const vshCmd *);    /* command handler */
    const vshCmdOptDef *opts;   /* definition of command options */
    const vshCmdInfo *info;     /* details about command */
    unsigned int flags;         /* bitwise OR of VSH_CMD_FLAG */
} vshCmdDef;

/*
 * vshCmd - parsed command
 */
typedef struct __vshCmd {
    const vshCmdDef *def;       /* command definition */
    vshCmdOpt *opts;            /* list of command arguments */
    struct __vshCmd *next;      /* next command */
} __vshCmd;

/*
 * vshControl
 */
typedef struct __vshControl {
    char *name;                 /* connection name */
    virConnectPtr conn;         /* connection to hypervisor (MAY BE NULL) */
    vshCmd *cmd;                /* the current command */
    char *cmdstr;               /* string with command */
    bool imode;                 /* interactive mode? */
    bool quiet;                 /* quiet mode */
    int debug;                  /* print debug messages? */
    bool timing;                /* print timing info? */
    bool readonly;              /* connect readonly (first time only, not
                                 * during explicit connect command)
                                 */
    char *logfile;              /* log file name */
    int log_fd;                 /* log file descriptor */
    char *historydir;           /* readline history directory name */
    char *historyfile;          /* readline history file name */
    bool useGetInfo;            /* must use virDomainGetInfo, since
                                   virDomainGetState is not supported */
    bool useSnapshotOld;        /* cannot use virDomainSnapshotGetParent or
                                   virDomainSnapshotNumChildren */
    virThread eventLoop;
    virMutex lock;
    bool eventLoopStarted;
    bool quit;

    const char *escapeChar;     /* String representation of
                                   console escape character */
} __vshControl;

typedef struct vshCmdGrp {
    const char *name;    /* name of group, or NULL for list end */
    const char *keyword; /* help keyword */
    const vshCmdDef *commands;
} vshCmdGrp;

void vshError(vshControl *ctl, const char *format, ...)
    ATTRIBUTE_FMT_PRINTF(2, 3);
bool vshInit(vshControl *ctl);
bool vshDeinit(vshControl *ctl);
void vshUsage(void);
void vshOpenLogFile(vshControl *ctl);
void vshOutputLogFile(vshControl *ctl, int log_level, const char *format,
                      va_list ap)
    ATTRIBUTE_FMT_PRINTF(3, 0);
void vshCloseLogFile(vshControl *ctl);

bool vshParseArgv(vshControl *ctl, int argc, char **argv);

const char *vshCmddefGetInfo(const vshCmdDef *cmd, const char *info);
const vshCmdDef *vshCmddefSearch(const char *cmdname);
bool vshCmddefHelp(vshControl *ctl, const char *name);
const vshCmdGrp *vshCmdGrpSearch(const char *grpname);
bool vshCmdGrpHelp(vshControl *ctl, const char *name);

int vshCommandOpt(const vshCmd *cmd, const char *name, vshCmdOpt **opt)
    ATTRIBUTE_NONNULL(1) ATTRIBUTE_NONNULL(2) ATTRIBUTE_NONNULL(3)
    ATTRIBUTE_RETURN_CHECK;
int vshCommandOptInt(const vshCmd *cmd, const char *name, int *value)
    ATTRIBUTE_NONNULL(3) ATTRIBUTE_RETURN_CHECK;
int vshCommandOptUInt(const vshCmd *cmd, const char *name,
                      unsigned int *value)
    ATTRIBUTE_NONNULL(3) ATTRIBUTE_RETURN_CHECK;
int vshCommandOptUL(const vshCmd *cmd, const char *name,
                    unsigned long *value)
    ATTRIBUTE_NONNULL(3) ATTRIBUTE_RETURN_CHECK;
int vshCommandOptString(const vshCmd *cmd, const char *name,
                        const char **value)
    ATTRIBUTE_NONNULL(3) ATTRIBUTE_RETURN_CHECK;
int vshCommandOptLongLong(const vshCmd *cmd, const char *name,
                          long long *value)
    ATTRIBUTE_NONNULL(3) ATTRIBUTE_RETURN_CHECK;
int vshCommandOptULongLong(const vshCmd *cmd, const char *name,
                           unsigned long long *value)
    ATTRIBUTE_NONNULL(3) ATTRIBUTE_RETURN_CHECK;
int vshCommandOptScaledInt(const vshCmd *cmd, const char *name,
                           unsigned long long *value, int scale,
                           unsigned long long max)
    ATTRIBUTE_NONNULL(3) ATTRIBUTE_RETURN_CHECK;
bool vshCommandOptBool(const vshCmd *cmd, const char *name);
const vshCmdOpt *vshCommandOptArgv(const vshCmd *cmd,
                                   const vshCmdOpt *opt);
char *vshGetDomainDescription(vshControl *ctl, virDomainPtr dom,
                              bool title, unsigned int flags)
    ATTRIBUTE_NONNULL(1) ATTRIBUTE_NONNULL(2) ATTRIBUTE_RETURN_CHECK;

# define VSH_BYID     (1 << 1)
# define VSH_BYUUID   (1 << 2)
# define VSH_BYNAME   (1 << 3)
# define VSH_BYMAC    (1 << 4)

virDomainPtr vshCommandOptDomainBy(vshControl *ctl, const vshCmd *cmd,
                                   const char **name, int flag);

/* default is lookup by Id, Name and UUID */
# define vshCommandOptDomain(_ctl, _cmd, _name)                      \
    vshCommandOptDomainBy(_ctl, _cmd, _name, VSH_BYID|VSH_BYUUID|VSH_BYNAME)

void vshPrintExtra(vshControl *ctl, const char *format, ...)
    ATTRIBUTE_FMT_PRINTF(2, 3);
void vshDebug(vshControl *ctl, int level, const char *format, ...)
    ATTRIBUTE_FMT_PRINTF(3, 4);

/* XXX: add batch support */
# define vshPrint(_ctl, ...)   vshPrintExtra(NULL, __VA_ARGS__)

/* User visible sort, so we want locale-specific case comparison.  */
# define vshStrcasecmp(S1, S2) strcasecmp(S1, S2)

int vshDomainState(vshControl *ctl, virDomainPtr dom, int *reason);
bool vshConnectionUsability(vshControl *ctl, virConnectPtr conn);
virTypedParameterPtr vshFindTypedParamByName(const char *name,
                                             virTypedParameterPtr list,
                                             int count);
char *vshGetTypedParamValue(vshControl *ctl, virTypedParameterPtr item)
    ATTRIBUTE_NONNULL(1) ATTRIBUTE_NONNULL(2);

char *editWriteToTempFile(vshControl *ctl, const char *doc);
int   editFile(vshControl *ctl, const char *filename);
char *editReadBackFile(vshControl *ctl, const char *filename);

/* Typedefs, function prototypes for job progress reporting.
 * There are used by some long lingering commands like
 * migrate, dump, save, managedsave.
 */
typedef struct __vshCtrlData {
    vshControl *ctl;
    const vshCmd *cmd;
    int writefd;
} vshCtrlData;

typedef void (*jobWatchTimeoutFunc) (vshControl *ctl, virDomainPtr dom,
                                     void *opaque);

void *_vshMalloc(vshControl *ctl, size_t sz, const char *filename, int line);
# define vshMalloc(_ctl, _sz)    _vshMalloc(_ctl, _sz, __FILE__, __LINE__)

void *_vshCalloc(vshControl *ctl, size_t nmemb, size_t sz,
                 const char *filename, int line);
# define vshCalloc(_ctl, _nmemb, _sz) \
    _vshCalloc(_ctl, _nmemb, _sz, __FILE__, __LINE__)

char *_vshStrdup(vshControl *ctl, const char *s, const char *filename,
                 int line);
# define vshStrdup(_ctl, _s)    _vshStrdup(_ctl, _s, __FILE__, __LINE__)

/* Poison the raw allocating identifiers in favor of our vsh variants.  */
# undef malloc
# undef calloc
# undef realloc
# undef strdup
# define malloc use_vshMalloc_instead_of_malloc
# define calloc use_vshCalloc_instead_of_calloc
# define realloc use_vshRealloc_instead_of_realloc
# define strdup use_vshStrdup_instead_of_strdup

extern virErrorPtr last_error;

#endif /* VIRSH_H */