#!/usr/bin/env python3

from twisted.internet import reactor

from pysnmp.entity import engine, config

from pysnmp.carrier.twisted.dgram import udp

from pysnmp.entity.rfc3413 import ntfrcv

import paramiko

import time

#device ip username and passwd

username = "*****"

passwd = "******"

ip = '192.168.45.36'

#comands

cmdup = ['sys','traffic-policy','rule name spark_hz>sh','disable','y',' rule name spark_sh>hz','disable','y']

cmddown = ['sys','traffic-policy','rule name spark_hz>sh','enable','rule name spark_sh>hz','enable']

#ssh device

def ssh2(ip,username,passwd,cmd):

    try:

        ssh = paramiko.SSHClient()

        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(ip,22,username,passwd,timeout=5)

        ssh_shell = ssh.invoke_shell()

        print(ssh_shell.recv(1024))

        for m in cmd:

            res = ssh_shell.sendall(m+'\n')

            time.sleep(float(1))

        print(ssh_shell.recv(1024))

        ssh.close()

    except :

        print('%s\tError\n'%(ip))

# Create SNMP engine with autogenernated engineID and pre-bound

# to socket transport dispatcher

snmpEngine = engine.SnmpEngine()

# Transport setup

# UDP over IPv4, first listening interface/port

config.addTransport(

    snmpEngine,

    udp.domainName + (1,),

    udp.UdpTwistedTransport().openServerMode(('0.0.0.0', 162))

)

# SNMPv1/2c setup

# SecurityName <-> CommunityName mapping

config.addV1System(snmpEngine, 'my-area', 'public')

# Callback function for receiving notifications

# noinspection PyUnusedLocal,PyUnusedLocal,PyUnusedLocal

def cbFun(snmpEngine, stateReference, contextEngineId, contextName,

          varBinds, cbCtx):

#Do what you want to do

     for  name, val in varBinds:
         print('-' * 30)
         print('name: %s' % name.prettyPrint())
         print('val: %s ' %  val.prettyPrint())
         print('-' * 30)

         if  val.prettyPrint()=='hz_sh_master':

               for name, val in varBinds:

                    if  val.prettyPrint()=='down':

                         print("down")

                         #cmd=cmddown

                         #ssh2(ip,username,passwd,cmd)

                    elif  val.prettyPrint()=='up':

                         print("up")

                         #cmd=cmdup

                         #ssh2(ip,username,passwd,cmd)

                    else:

                         pass

         else:

               pass

# Register SNMP Application at the SNMP engine

ntfrcv.NotificationReceiver(snmpEngine, cbFun)

# Run Twisted main loop

reactor.run()

# Register SNMP Application at the SNMP engine

ntfrcv.NotificationReceiver(snmpEngine, cbFun)

# Run Twisted main loop

reactor.run()
