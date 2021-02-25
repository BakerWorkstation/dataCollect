#!/usr/bin/env python3

from pysnmp.hlapi import *

errorIndication, errorStatus, errorIndex, varBinds = next(
    sendNotification(
        SnmpEngine(OctetString(hexValue='8000000001020304')),
        UsmUserData('usr-sha-aes128', 'authkey1', 'privkey1',
                    authProtocol=USM_AUTH_HMAC96_SHA,
                    privProtocol=USM_PRIV_CFB128_AES),
        UdpTransportTarget(('localhost', 162)),
        ContextData(),
        'trap',
        NotificationType(ObjectIdentity('SNMPv2-MIB', 'authenticationFailure'))
    )
)

if errorIndication:
    print(errorIndication)
