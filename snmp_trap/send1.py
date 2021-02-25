#/usr/bin/env python3


# Notification Originator Application (TRAP)
from pysnmp.carrier.asynsock.dispatch import AsynsockDispatcher
from pysnmp.carrier.asynsock.dgram import udp
from pyasn1.codec.ber import encoder
from pysnmp.proto import api
# Protocol version to use
verID = api.protoVersion1
pMod = api.protoModules[verID]
# Build PDU
trapPDU =  pMod.TrapPDU()
pMod.apiTrapPDU.setDefaults(trapPDU)
# Traps have quite different semantics among proto versions
#if verID == api.protoVersion1:
pMod.apiTrapPDU.setEnterprise(trapPDU, (1,3,6,1,6,3,1,1,5,1))
pMod.apiTrapPDU.setGenericTrap(trapPDU, 'coldStart')
# Build message
trapMsg = pMod.Message()
pMod.apiMessage.setDefaults(trapMsg)
pMod.apiMessage.setCommunity(trapMsg, 'public')
pMod.apiMessage.setPDU(trapMsg, trapPDU)
transportDispatcher = AsynsockDispatcher()
transportDispatcher.registerTransport(
    udp.domainName, udp.UdpSocketTransport().openClientMode()
    )
transportDispatcher.sendMessage(
    encoder.encode(trapMsg), udp.domainName, ('10.255.175.109', 162)
    )
transportDispatcher.runDispatcher()
transportDispatcher.closeDispatcher()
