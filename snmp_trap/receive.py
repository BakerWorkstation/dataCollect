#!/usr/bin/env python3.6

#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys

sys.path.append(".")


from pysnmp.entity import engine, config
from pysnmp.proto import api

from pyasn1.codec.ber import decoder
from pysnmp.carrier.asynsock.dgram import udp, udp6

from pysnmp.carrier.asynsock.dispatch import AsynsockDispatcher

from pysnmp.entity.rfc3413 import ntfrcv
# try:
#     from monogdb_api import Logsystem
# except:
#     from .monogdb_api import Logsystem

#Logsystem = Logsystem()

snmpEngine = engine.SnmpEngine()

def cbFun(transportDispatcher, transportDomain, transportAddress, wholeMsg):
    while wholeMsg:
        msgVer = int(api.decodeMessageVersion(wholeMsg))
        print(msgVer)
        if msgVer in api.protoModules:
            pMod = api.protoModules[msgVer]
        else:
            print('Unsupported SNMP version %s' % msgVer)
            return
        reqMsg, wholeMsg = decoder.decode(
            wholeMsg, asn1Spec=pMod.Message(),
            )
        print('Notification message from %s:%s: ' % (
            transportDomain, transportAddress
            )
        )
        ipdress = transportAddress[0]
        reqPDU = pMod.apiMessage.getPDU(reqMsg)
        if reqPDU.isSameTypeWith(pMod.TrapPDU()):
            strlist = ""
            if msgVer == api.protoVersion1:
                try:
                    strlist = 'Enterprise: {},Agent Address: {},Generic Trap: {},Specific Trap: {},Uptime: {}'.format(
                        pMod.apiTrapPDU.getEnterprise(reqPDU).prettyPrint(),
                        pMod.apiTrapPDU.getAgentAddr(reqPDU).prettyPrint(),
                        pMod.apiTrapPDU.getGenericTrap(reqPDU).prettyPrint(),
                        pMod.apiTrapPDU.getSpecificTrap(reqPDU).prettyPrint(),
                        pMod.apiTrapPDU.getTimeStamp(reqPDU).prettyPrint()
                        )
                    #
                except Exception as e:
                    print(str(e))
                    strlist = ("messages error,error({})").format(e)
                varBinds = pMod.apiTrapPDU.getVarBindList(reqPDU)
            else:
                strlist= "error messages is not"
                varBinds = pMod.apiPDU.getVarBindList(reqPDU)
            print(strlist)
        #Logsystem.error((u"ip ({}) is receive  trap error, Please check the device details immediately.\n {}").format(ipdress,strlist))

            print('Var-binds:')
            print(str(varBinds))
            # for oid, val in varBinds:
            #     #a = oid.prettyPrint().strip()
            #     b = val.prettyPrint().strip().split('\n')
            #     #print(a)
            #     for line in b:
            #         item = line.strip()
            #         if item.startswith('string-value'):
            #             print('string-value='+item.replace('string-value=0x','').decode('hex'))
            #         else:
            #             print(item)
    return wholeMsg


if __name__ == '__main__':
    transportDispatcher = AsynsockDispatcher()

    transportDispatcher.registerRecvCbFun(cbFun)

    # UDP/IPv4
    transportDispatcher.registerTransport(
        udp.domainName, udp.UdpSocketTransport().openServerMode(('0.0.0.0', 162))
    )

    # UDP/IPv6
    transportDispatcher.registerTransport(
        udp6.domainName, udp6.Udp6SocketTransport().openServerMode(('::1', 162))
    )

    transportDispatcher.jobStarted(1)

    try:
        # Dispatcher will never finish as job#1 never reaches zero
        transportDispatcher.runDispatcher()
    except:
        transportDispatcher.closeDispatcher()
        raise