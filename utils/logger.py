import logging
from colorlog import ColoredFormatter
from config import LOG_LEVEL
# import helpers.extendLogger

# #########################
# # Logging
# logging.basicConfig(
#     format='[%(asctime)s] [%(levelname)s] [THREAD_ID %(thread)d] [PROCESS_ID %(process)d] :%(message)s',
#     level=logging.DEBUG
#     )
# #########################

def printWelcomeMessage():
    print("""                                                                                
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,:ccccc:'';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;',dkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;'':cccccc:'';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,'cxkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkxkkxxxxx
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,';cccccccc;',;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;';dkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkxxdoooooooooo
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,',cccccccccc,',;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,,okkkkkkkkkkkkkkkkkkkkkkkkkkkkkkxxkkxxxxxxxxxxxxxxollcccccccccc
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;'':ccccccccccc,';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;':xkkkkkkkkkkkkkkkkkkxkkkxxddooooooooooddddddoooooollllllllllll
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;'':cccccccccccc:'';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;';dkkkkkkkkkkkkkkkkkkkkkxdllccccclloooooooooooooooooooooooooooo
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,.;cccccccccccccc;.,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,'lxkkkkkkkkkkkkkkkkkkkxdlclllooooooooooooooooooooooooooooooooo
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;',ccccccccccccccc:'';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;':xkkkkkkkxxddddddddddoolloooooooooooooooooooooooooooooooooooo
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;'':cccccccccccccccc;',;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,,okkkkkxdollccclloooooooooooooooooooooooooooooooooooooooooooo
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,';cccccccccccccccccc'';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;'cxxdooollccccclooooooooooooooooooooooooooooooooooooooooooooo
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,',ccccccccccccccccccc;',;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;',lllcclloooooooooooooooooooooooooooooooooooooooooooooooooooo
;;;;;;;;;;;;;;;;;;;;;;;;;'..,;;;'':ccccccccccccccccccc:'';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,';cccloooooooooooooooooooooooooooooooooooooooooooooooooooool
;;;;;;;;;;;;;;;;;;;;;;;;'.   ....:ccccccccccccccccccccc,',;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;',ccccloooooooooooooooooooooooooooooooooooooooooooooooooollc
;;;;;;;;;;;;;;;;;;;;;;;;,.       .,:ccccccccccccccccccc:'';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,':lcccclooooooooooooooooooooooooooooooooooooooooooooolllccc
;;;;;;;;;;;;;;;;;;;;;;;;;,.         .';cccc::;::cccccccc,';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;';ollllooooooooooooooooooooooooooooooooooooooooooooollccccc
;;;;;;;;;;;;;;;;;;;;;;;;;;;,.         ,:;,,,,,,,,;;;::::,.,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;',clloooooooooooooooooooooooooooooooooooooooooooooolllccccc
;;;;;;;;;;;;;;;;;;;;;;;;;;;;,.       .lOxl:,'',,,,,,,,,,'.';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,':llllooooooooooooooooooooooooooooooooooooooooollllllccccc
;;;;;;;;;;;;;;;;;;;;;;;;;;;,',;.     .oOOOOxoc;''',,,,,,,.';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;',llllllllooooooooooooooooooooooooooooooolllllllc;,,,;cccc
;;;;;;;;;;;;;;;;;;;;;;;;;;;''cc;.    .d0OOOOOOkoc;''',,,,..,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;''clllllllllllllllllooooooooooolllllllllcc:;,'...    .;ccc
;;;;;;;;;;;;;;;;;;;;;;;;;;'':ccc:.   .oOOOOOOOOOOkdl:,'','.,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,';llllllllllllllllllllllllllllllc::::ccl;.          'cccc
;;;;;;;;;;;;;;;;;;;;;;;;;,';ccccc:.   :OOOOOOOOOOOOOOxl:,..';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;',clllllllllllllllllllllcc::::ccllodxkOOx,         .:cccc
;;;;;;;;;;;;;;;;;;;;;;;;;',ccccccc:.  ,k0OOOOOOOOOO0OOOOko:,,,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,':lllllllllllllllcc:::ccloddkOOOOOOOOOOk;        ':ccccc
;;;;;;;;;;;;;;;;;;;;;;;;'':cccccccc:. .oOOOOOOOOOOOOOOOOOOOkdl:,,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;',llllllllllc:::cllodxOOOOOOOOOOOOOOOO0x'       'ccccccc
;;;;;;;;;;;;;;;;;;;;;;;,';cccccccccc:. ;kOOOOOOOOOOOOOOOOOOOOOOdl:,,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;''cllllc:::clodxOOOOOOOOOOOOOOOOOOOOOOOl.      ':ccccccc
;;;;;;;;;;;;;;;;;;;;;;;',cccccccccccc:..oOOOOOOOOOOOOOOOOOOOOOOOOkd:,,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;,';lc::cldkOOOOOOOOOOOOOOOOOOOOOOOOOO0x,      'ccccccccc
;;;;;;;;;;;;;;;;;;;;;;'.,:;::::::ccccc:,;xOOOOOOOOOOOOOOOOOOOOOOOOOOd:,,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;.':ldkO0OOOOOOOOOOOOOOOOOOOOOOOOOOOOOc.    .,cccccccccc
;;;;;;;;;;;;;;;;;;;;;,..,,,,,,,,,,;:cccc;;oOOOOOOOOOOOOOOOOOOOOOOOOOOOd;,;;;;;;;;;;;;;;;;;;;;;,,,,,,,,,,,,,,,,,,;;;;;,;cdkOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOd.    .;ccccccccccc
;;;;;;;;;;;;;;;;;;;;;..',,,,,,,,,,,,;;;;;;;cxOOOOOOOOOOOOOOOOOOOOOOOOOOkl,,;;;;;,,,,,;;;:::cccccclllllllllllllccccc::lxOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOk,   .':cccccc::;;;;
;;;;;;;;;;;;;;;;;;;;'.',,,,,,,,,,,,,,,,,,,;,,lkOOOOOOOOOOOOOOOOOOOOOOOOOOd:;;::clloodxxkkOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOk:   .:ccccccc:;;;;;;
;;;;;;;;;;;;;;;;;;;;..,,,,,,,,,,,,,,,,,,,,,,'';okOOOOOOOOOOOOOOOOOOOOOOOOOkkkOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOk:..';cccccccc:;;;;;;;
;;;;;;;;;;;;;;;;;;;'.',,,,,,,,,,,,,,,,,,,,,,,,'';okOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOx:..;::::;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;,..,,,,,,,,,,,,,,,,,,,,,,,,,,,'';lxOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOko:,',;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;'.',,,,,,,,,,,,,,,,,,''''''''',,'',cdOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOxoc;,,,;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;,..,,,,,,,,,,,,,,,'.............,,,,',:okOkddkOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOkdl:,',,;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;..',,,,,,,,,,,,,,,..............',,,,,'',:loxOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOxoc;,',,;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;,.',,,,,,,,,,,,,''................,,,,,,''ckOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOdlxOxoc:,',,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;..,,,,,,,,,,,,,'..................',,,,';dOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOo;;,'',;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;,.',,,,,,,,,,,,,'.................',,,,';xOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOo,';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;..,,,,,,,,,,,,,,,,'''''''''''''''',,,,';xOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOkkkOOOOOOOOOOOOOOOOOOOOo,';,;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;,.',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,',dOOOOOOOOOkxddollxOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOxdxxdc;cdOOOOOOOOOOOOOOOOOOo,',;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;..,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,''lOOOOOOOOOdcdKXKo'.:xOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOo;xNMWK:..ckOOOOOOOOOOOOOOO0Oo,';;;;;;;;;;;;;;;;;;,;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;,.',,,,,,'''''''''''''''''',,,,,,,,,,,.;k0OOOOOOOx;;OMMM0;..,dOOOOOOOOOOOOOOOOOOOOOOOOOOOOO0k:.oXWNO;...lO0OOOOOOOOOOOOOOOOl',;;;;;;;;;;;;;;;;;;;;;;::::::;;;;;;;
;;;;;;;;;;;;;..',,,,'.....................,,,,,,,,',oOOOOOOOOOx;.;oxo;....cOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOkc..,:;.....;kOOOOOOOOOOOOOOOOOkc',;;;;;;;;;;;;;;;;;;:ccccccccccccccc
;;;;;;;;;;;;,..,,,''.......................',,,,,''ckOOOOOOOO0Ol..........lOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOx:........'lOOOOOOOOOOOOOOOOOOOk:',;;;;;;;;;;;;;;;;:cccccccccccccccc
;;;;;;;;;;;;'...............................',,,,',xOOOOOOOOOOOOo;......,lkOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOko:,..',cxOOOOOOOOOOOOOOOOOOOOOd;';;;;;;;;;;;::;::ccccccccccccccccc
;;;;;;;;;;;,................................',,,''lOOOOOOOOOOOOOOOxolcldkOOOOOOOOOkdccloxOOOOOOOOOOOOOOOOOOOOOkxxxkOOOOOOOOOOOOOOOOOOOOOOOOo,,;;;;;;:ccccccccccccccccccccccccc
;;;;;;;;;;;'.............................'',,,,,.:kOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOkc.....;oOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOkc',;;;;:cccccccccccccccccccccccccc
;;;;;;;;;;,............................',,,,,,,''lxdddddxOO0OOOOOOOOOOOOOOOOOOOOOOOkdc;,',lkOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOx;';;;;ccccccccccccccccccccccccccc
;;;;;;;;;;'........................',,,,,,,,,,'.'ccccccccldOOOOOOOOOOOOOOOOOOOOOOOOOOOOkkkOOOOOOOOOOOOOOOOOOOOOOOOOOOOOkdolllllodxkOOOOOOOOOOl',;;;:cccccccccccccccccccccccccc
;;;;;;;;;;..,,,,''''................',,,,,,,,,..:clccccccccoOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOxlcccccccccclokOOOOOOO0k:',;;:cccccccccccccccccccccccccc
;;;;;;;;;'.',,,,,,,,,,...............,,,,,,,,'.,ccccccccccccdOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO0OOOOOOOOOOOOOOOOOOOOOOxcccccccccccccccxOOOOOO0Oo,';;ccccccccccccccccccccccccccc
;;;;;;;;;..,,,,,,,,,,,,'............',,,,,,,,'':ccccccccccc:lkOOOOOOOOOOOOOOOOOOOOOkdocclooooooddxkOOOOOOOOOOOOOOOOOOocccccclccccccc:oOOOOOOOOk:',;ccccccccccccccccccccccccccc
;;;;;;;;,..,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,;:'':ccccccccccc:lkOOOOOOOOOOOOOOOOOOOkl,..,coddddoollllodkOOOOOOOOOOOOOOOdccccccclcccccccdOOOOOOOOOo,';:cccccccccccccccccccccccccc
;;;;;;;;'.',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,.;lccccccccccccoOOOOOOOOOOOOOOOOOOOkc...:oddddddddddddoclkOOOOOOOOOOOOOOkocccccccllccccokOOOOOOOO0k:',;;;::::::::::::::::cccccccc
;;;;;;;,..,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,.;xxolcccccccldOOOOOOOOOOOOOOOOOOO0x,.'cdxdddddddddddxxocoOOOOOOOOOOOOOOOkdccccccccccldkOOOOOOOOOOOo,,;;;;;;;;;;;;;;;;;;;;;;;;;;:
;;;;;;;'.',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,''oOOOkxddddxkOOOOOOOOOOOOOOOOOOOOOx;'cdxddddddxddddddddllkOOOOOOOOOOOOOOOOkdollllloxkOOOOOOOOOOOOOk:',;;;;;;;;;;;;;;;;;;;;;;;;;:
;;;;;;,..,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,',dOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOo:ldddddddxxddddddddllkOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOl',;;;;;;;;;;;;;;;;;;;;;;;;;:
;;;;;;'.',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,;;;;;;::,;dOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOdllodddddddddddddxdcoOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO0x;';;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;..,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,:ccccccccc;;oOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOkdolllloodddddoollok0OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOc',;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;'.',,,,,,,,,,,,,,,,,,,,,,,,;;;;;:ccccccccccc:;lOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO0OkxddooooooooodkOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOo,,;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;..,,,,,,,,,,,,,,,,,,,,,,,,;:ccccccccccccccccc:;lkOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO0x;';;;;;;;;;;;;;;;;;;;;;;;;;
;;;;,.',,,,,,,,,,,,,,,,,,,,,,,,,,:ccccccccccccccccc:;lOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOc',;;;;;;;;;;;;;;;;;;;;;;;;
;;;;'.',,,,,,,,,,,,,,,,,,,,,,,,,,:cccccccccccccccccc:;dOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOo,,;;;;;;;;;;;;;;;;;;;;;;;;
;;;,..,,,,,,,,,,,,,,,,,,,,,,,,,,,;:cccccccccccccccccc,ckOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOx;';;;;;;;;;;;;;;;;;;;;;;;;
;;;'.',,,,,,,,,,,,,,,,,,,,,,,,,,,,,;:cccccccccccccccc;;x0OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOc',;;;;;;;;;;;;;;;;;;;;;;;
;;,..,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,:cccccccccccccccc;;d0OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOo',;;;;;;;;;;;;;;;;;;;;;;;
;;'.',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,;:ccccccccccccccc;;x0OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOx;';;;;;;;;;;;;;;;;;;;;;;;
;,..,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,:cccccccccccccc;ckOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO0k:',;;;;;;;;;;;;;;;;;;;;;;
;'.',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,;ccccccccccccc:,lOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOc',;;;;;;;;;;;;;;;;;;;;;;

    """)
    print('__________________________________________________________________')
    print('Manga crawler for mangadex.org')
    print('__________________________________________________________________')
    print('VERSION: 0.1')
    print('AUTHOR: stiendat (http://github.com/stiendat)')
    print('__________________________________________________________________')

    print('Hello onii-chan! (❀˘꒳˘)♡(˘꒳˘❀)')
    print('''
    
    ''')


def getLogLevel(loglevel):
    if loglevel == 'warn':
        return logging.WARN
    elif loglevel == 'debug':
        return logging.DEBUG
    elif logging == 'error':
        return logging.ERROR
    else:
        return logging.INFO

logging.root.setLevel(logging.DEBUG)
formatter = ColoredFormatter('%(log_color)s[%(asctime)s] [%(levelname)s] [THREAD_ID %(thread)d] [PROCESS_ID %(process)d] :%(message)s%(reset)s')
stream = logging.StreamHandler()
stream.setFormatter(formatter)
logger = logging.getLogger('basicLogger')
logger.addHandler(stream)
logger.setLevel(getLogLevel(LOG_LEVEL))

def getLogLevel(loglevel):
    if loglevel == 'warn':
        return logging.WARN
    elif loglevel == 'debug':
        return logging.DEBUG
    elif logging == 'error':
        return logging.ERROR
    else:
        return logging.INFO

def info(logData, danger = None):
    logger.info(str(logData))

def warn(logData, danger = None):
    logger.warning(str(logData))

def debug(logData, danger = None):
    logger.debug(str(logData))

def error(logData, danger = None):
    logger.error(str(logData))