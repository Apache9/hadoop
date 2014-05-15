/*
 * %W% %E%
 *
 * Copyright (c) 2006, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package sun.security.krb5;

import java.io.IOException;
import java.io.PrintStream;

import sun.security.krb5.internal.APOptions;
import sun.security.krb5.internal.APReq;
import sun.security.krb5.internal.Authenticator;
import sun.security.krb5.internal.AuthorizationData;
import sun.security.krb5.internal.EncTicketPart;
import sun.security.krb5.internal.HostAddress;
import sun.security.krb5.internal.HostAddresses;
import sun.security.krb5.internal.KRBError;
import sun.security.krb5.internal.KdcErrException;
import sun.security.krb5.internal.KerberosTime;
import sun.security.krb5.internal.Krb5;
import sun.security.krb5.internal.KrbApErrException;
import sun.security.krb5.internal.LocalSeqNumber;
import sun.security.krb5.internal.SeqNumber;
import sun.security.krb5.internal.Ticket;
import sun.security.krb5.internal.TicketFlags;
import sun.security.krb5.internal.crypto.EType;
import sun.security.krb5.internal.rcache.AuthTime;
import sun.security.krb5.internal.rcache.CacheTable;
import sun.security.util.DerValue;

public class KrbApReq
{
  private byte[] obuf;
  private KerberosTime ctime;
  private int cusec;
  private Authenticator authenticator;
  private Credentials creds;
  private APReq apReqMessg;
  private static CacheTable table = new CacheTable();

  private static boolean DEBUG = Krb5.DEBUG;

  public KrbApReq(Credentials paramCredentials,
      boolean paramBoolean1, boolean paramBoolean2,
      boolean paramBoolean3, Checksum paramChecksum)
    throws Asn1Exception, KrbCryptoException,
           KrbException, IOException
  {
    APOptions localAPOptions =
      paramBoolean1 ? new APOptions(2) : new APOptions();

    if (DEBUG) {
      System.out.println(">>> KrbApReq: APOptions are " + localAPOptions);
    }
    EncryptionKey localEncryptionKey =
      paramBoolean2 ? new EncryptionKey(paramCredentials.getSessionKey()) : null;

    LocalSeqNumber localLocalSeqNumber = new LocalSeqNumber();

    init(localAPOptions, paramCredentials, paramChecksum,
        localEncryptionKey, localLocalSeqNumber, null, 11);
  }

  public KrbApReq(byte[] paramArrayOfByte,
      EncryptionKey[] paramArrayOfEncryptionKey)
    throws KrbException, IOException
  {
    this.obuf = paramArrayOfByte;
    if (this.apReqMessg == null)
      decode();
    authenticate(paramArrayOfEncryptionKey, null);
  }

  KrbApReq(APOptions paramAPOptions,
      Ticket paramTicket, EncryptionKey paramEncryptionKey1,
      Realm paramRealm, PrincipalName paramPrincipalName,
      Checksum paramChecksum, KerberosTime paramKerberosTime,
      EncryptionKey paramEncryptionKey2, SeqNumber paramSeqNumber,
      AuthorizationData paramAuthorizationData)
    throws Asn1Exception, IOException, KdcErrException, KrbCryptoException
  {
    init(paramAPOptions, paramTicket, paramEncryptionKey1, paramRealm,
        paramPrincipalName, paramChecksum, paramKerberosTime,
        paramEncryptionKey2, paramSeqNumber, paramAuthorizationData, 7);
  }

  private void init(APOptions paramAPOptions, Credentials paramCredentials,
      Checksum paramChecksum, EncryptionKey paramEncryptionKey,
      SeqNumber paramSeqNumber, AuthorizationData paramAuthorizationData,
      int paramInt) throws KrbException, IOException
  {
    this.ctime = new KerberosTime(true);
    init(paramAPOptions, paramCredentials.ticket, paramCredentials.key,
        paramCredentials.client.getRealm(), paramCredentials.client,
        paramChecksum, this.ctime, paramEncryptionKey, paramSeqNumber,
        paramAuthorizationData, paramInt);
  }

  private void init(APOptions paramAPOptions, Ticket paramTicket,
      EncryptionKey paramEncryptionKey1, Realm paramRealm,
      PrincipalName paramPrincipalName, Checksum paramChecksum,
      KerberosTime paramKerberosTime, EncryptionKey paramEncryptionKey2,
      SeqNumber paramSeqNumber, AuthorizationData paramAuthorizationData,
      int paramInt)
    throws Asn1Exception, IOException, KdcErrException, KrbCryptoException
  {
    createMessage(paramAPOptions, paramTicket, paramEncryptionKey1, paramRealm,
        paramPrincipalName, paramChecksum, paramKerberosTime,
        paramEncryptionKey2, paramSeqNumber, paramAuthorizationData, paramInt);

    this.obuf = this.apReqMessg.asn1Encode();
  }

  void decode() throws KrbException, IOException
  {
    DerValue localDerValue = new DerValue(this.obuf);
    decode(localDerValue);
  }

  void decode(DerValue paramDerValue) throws KrbException, IOException {
    this.apReqMessg = null;
    try {
      this.apReqMessg = new APReq(paramDerValue);
    } catch (Asn1Exception localAsn1Exception) {
      this.apReqMessg = null;
      KRBError localKRBError = new KRBError(paramDerValue);
      String str1 = localKRBError.getErrorString();
      String str2;
      if (str1.charAt(str1.length() - 1) == 0)
        str2 = str1.substring(0, str1.length() - 1);
      else
        str2 = str1;
      KrbException localKrbException = new KrbException(
          localKRBError.getErrorCode(), str2);
      localKrbException.initCause(localAsn1Exception);
      throw localKrbException;
    }
  }

  private void authenticate(EncryptionKey[] paramArrayOfEncryptionKey,
      HostAddress paramHostAddress) throws KrbException, IOException
  {
    int i = this.apReqMessg.ticket.encPart.getEType();
    Integer localInteger = this.apReqMessg.ticket.encPart.getKeyVersionNumber();
    EncryptionKey localEncryptionKey = EncryptionKey.findKey(i,
        localInteger, paramArrayOfEncryptionKey);

    if (localEncryptionKey == null) {
      throw new KrbException(400,
          "Cannot find key of appropriate type to decrypt AP REP - "
          + EType.toString(i));
    }

    byte[] arrayOfByte1 = this.apReqMessg.ticket.encPart.decrypt(
        localEncryptionKey, 2);

    byte[] arrayOfByte2 = this.apReqMessg.ticket.encPart.reset(
        arrayOfByte1, true);
    EncTicketPart localEncTicketPart = new EncTicketPart(arrayOfByte2);

    checkPermittedEType(localEncTicketPart.key.getEType());

    byte[] arrayOfByte3 = this.apReqMessg.authenticator.decrypt(
        localEncTicketPart.key, 11);

    byte[] arrayOfByte4 = this.apReqMessg.authenticator.reset(
        arrayOfByte3, true);
    this.authenticator = new Authenticator(arrayOfByte4);
    this.ctime = this.authenticator.ctime;
    this.cusec = this.authenticator.cusec;
    this.authenticator.ctime.setMicroSeconds(this.authenticator.cusec);
    this.authenticator.cname.setRealm(this.authenticator.crealm);
    this.apReqMessg.ticket.sname.setRealm(this.apReqMessg.ticket.realm);
    localEncTicketPart.cname.setRealm(localEncTicketPart.crealm);

    if (!this.authenticator.cname.equals(localEncTicketPart.cname)) {
      throw new KrbApErrException(36);
    }
    KerberosTime localKerberosTime1 = new KerberosTime(true);
    if (!this.authenticator.ctime.inClockSkew(localKerberosTime1)) {
      throw new KrbApErrException(37);
    }

    // Turn off the replay attack detection during client<->server mutual authentication
    // AuthTime localAuthTime = new AuthTime(this.authenticator.ctime.getTime(),
    //     this.authenticator.cusec);

    // String str = this.authenticator.cname.toString();
    // if (table.get(localAuthTime, this.authenticator.cname.toString()) != null) {
    //   throw new KrbApErrException(34);
    // }
    // table.put(str, localAuthTime, localKerberosTime1.getTime());

    if ((paramHostAddress != null) &&
      (localEncTicketPart.caddr != null)) {
      if (paramHostAddress == null)
        throw new KrbApErrException(38);
      if (!localEncTicketPart.caddr.inList(paramHostAddress)) {
        throw new KrbApErrException(38);
      }

    }

    KerberosTime localKerberosTime2 = new KerberosTime(true);

    if (((localEncTicketPart.starttime != null) &&
          (localEncTicketPart.starttime.greaterThanWRTClockSkew(
              localKerberosTime2))) || (localEncTicketPart.flags.get(7)))
    {
      throw new KrbApErrException(33);
    }

    if ((localEncTicketPart.endtime != null) &&
        (localKerberosTime2.greaterThanWRTClockSkew(localEncTicketPart.endtime)))
    {
      throw new KrbApErrException(32);
    }

    this.creds = new Credentials(this.apReqMessg.ticket,
        this.authenticator.cname, this.apReqMessg.ticket.sname,
        localEncTicketPart.key, null, localEncTicketPart.authtime,
        localEncTicketPart.starttime, localEncTicketPart.endtime,
        localEncTicketPart.renewTill, localEncTicketPart.caddr);

    if (DEBUG)
      System.out.println(">>> KrbApReq: authenticate succeed.");
  }

  public Credentials getCreds()
  {
    return this.creds;
  }

  KerberosTime getCtime() {
    if (this.ctime != null)
      return this.ctime;
    return this.authenticator.ctime;
  }

  int cusec() {
    return this.cusec;
  }

  APOptions getAPOptions() throws KrbException, IOException {
    if (this.apReqMessg == null)
      decode();
    if (this.apReqMessg != null)
      return this.apReqMessg.apOptions;
    return null;
  }

  public boolean getMutualAuthRequired()
    throws KrbException, IOException
  {
    if (this.apReqMessg == null)
      decode();
    if (this.apReqMessg != null)
      return this.apReqMessg.apOptions.get(2);
    return false;
  }

  boolean useSessionKey() throws KrbException, IOException {
    if (this.apReqMessg == null)
      decode();
    if (this.apReqMessg != null)
      return this.apReqMessg.apOptions.get(1);
    return false;
  }

  public EncryptionKey getSubKey()
  {
    return this.authenticator.getSubKey();
  }

  public Integer getSeqNumber()
  {
    return this.authenticator.getSeqNumber();
  }

  public Checksum getChecksum()
  {
    return this.authenticator.getChecksum();
  }

  public byte[] getMessage()
  {
    return this.obuf;
  }

  public PrincipalName getClient()
  {
    return this.creds.getClient();
  }

  private void createMessage(APOptions paramAPOptions, Ticket paramTicket,
      EncryptionKey paramEncryptionKey1, Realm paramRealm,
      PrincipalName paramPrincipalName, Checksum paramChecksum,
      KerberosTime paramKerberosTime, EncryptionKey paramEncryptionKey2,
      SeqNumber paramSeqNumber, AuthorizationData paramAuthorizationData,
      int paramInt)
    throws Asn1Exception, IOException, KdcErrException, KrbCryptoException
  {
    Integer localInteger = null;

    if (paramSeqNumber != null) {
      localInteger = new Integer(paramSeqNumber.current());
    }
    this.authenticator = new Authenticator(paramRealm, paramPrincipalName,
        paramChecksum, paramKerberosTime.getMicroSeconds(), paramKerberosTime,
        paramEncryptionKey2, localInteger, paramAuthorizationData);

    byte[] arrayOfByte = this.authenticator.asn1Encode();

    EncryptedData localEncryptedData = new EncryptedData(paramEncryptionKey1,
        arrayOfByte, paramInt);

    this.apReqMessg = new APReq(paramAPOptions, paramTicket, localEncryptedData);
  }

  private static void checkPermittedEType(int paramInt)
    throws KrbException
  {
    int[] arrayOfInt = EType.getDefaults("permitted_enctypes");
    if (arrayOfInt == null) {
      throw new KrbException(
          "No supported encryption types listed in permitted_enctypes");
    }

    if (!EType.isSupported(paramInt, arrayOfInt))
      throw new KrbException(EType.toString(paramInt) +
          " encryption type not in permitted_enctypes list");
  }
}
