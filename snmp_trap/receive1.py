#!/usr/bin/env python3.6

from pysnmp.entity import engine, config
from pysnmp.carrier.asyncore.dgram import udp
from pysnmp.entity.rfc3413 import ntfrcv
import datetime
import os

fileout = "/tmp/zabbix_traps.tmp"

# Create SNMP engine with autogenernated engineID and pre-bound
# to socket transport dispatcher
snmpEngine = engine.SnmpEngine()

# Transport setup

# UDP over IPv4, first listening interface/port
config.addTransport(
    snmpEngine,
    udp.domainName + (1,),
    udp.UdpTransport().openServerMode(('0.0.0.0', 162))
)

# SNMPv1/2c setup

# SecurityName <-> CommunityName mapping
config.addV1System(snmpEngine, 'my-area', 'public')


# def responseAction(ip, var, val):
#     command = "snmpset -v2c -c private %s .1.3.6.1.4.1.24024.%d.0 i %d" % (ip, 50 + var, val)
#     os.system(command)
#     print("response:       " + command)


# Callback function for receiving notifications
# noinspection PyUnusedLocal,PyUnusedLocal
def cbFun(snmpEngine, stateReference, contextEngineId, contextName,
          varBinds, cbCtx):
    # Get an execution context...
    execContext = snmpEngine.observer.getExecutionContext(
        'rfc3412.receiveMessage:request'
    )

    # ... and use inner SNMP engine data to figure out peer address
    # print('Notification from %s, ContextEngineId "%s", ContextName "%s"' % ('@'.join([str(x) for x in execContext['transportAddress']]),
    #                                                                        contextEngineId.prettyPrint(),
    #                                                                        contextName.prettyPrint()))

    now = datetime.datetime.now().strftime("%H:%M:%S %Y-%m-%d")
    iporig = execContext['transportAddress'][0]
    # print('Notification from %s' % iporig    )

    trapoid = ""
    #trapcontent = "%s %s ZBXTRAP %s ELEM:- CAUSE:- VALUE:-" % (now, trapoid, iporig)
    trapcontent_varbinds = ""
    for name, val in varBinds:
        trapcontent_varbinds = "%s%s => %s\n" % (trapcontent_varbinds, name, val)
        print('%s = %s' % (name.prettyPrint(), val.prettyPrint()))

        if name.prettyPrint() == "1.3.6.1.6.3.1.1.4.1.0":
            trapoid = val.prettyPrint()

    trapcontent = "%s %s ZBXTRAP %s ELEM:- CAUSE:- VALUE:-\n%s" % (now, trapoid, iporig, trapcontent_varbinds)
    #print("---------trapcontent:" + trapcontent)

    # f = open(fileout, 'a')
    # f.write(trapcontent)
    # f.close()

    # if (trapoid == "1.3.6.1.4.1.24024.100" and "TRAP_ERROR" in trapcontent):
    #     # red down =>  UPS ON, GE ON
    #     responseAction(iporig, 1, 1)
    #     responseAction(iporig, 3, 1)
    #
    # if (trapoid == "1.3.6.1.4.1.24024.100" and "TRAP_CLEAR" in trapcontent):
    #     # red up =>  UPS OFF , GE OFF
    #     responseAction(iporig, 1, 0)
    #     responseAction(iporig, 3, 0)


# Register SNMP Application at the SNMP engine
ntfrcv.NotificationReceiver(snmpEngine, cbFun)

snmpEngine.transportDispatcher.jobStarted(1)  # this job would never finish

# Run I/O dispatcher which would receive queries and send confirmations
try:
    snmpEngine.transportDispatcher.runDispatcher()
except:
    snmpEngine.transportDispatcher.closeDispatcher()
    raise