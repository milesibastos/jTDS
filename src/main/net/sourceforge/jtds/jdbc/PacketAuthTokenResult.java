package net.sourceforge.jtds.jdbc;

/**
 * Created by mdb to hold the result of processing the token TDS_AUTH_TOKEN,
 * which is the NTLM challenge sent as a part of authenticating to MS SQL Server
 * using NTLM auth.
 */
public class PacketAuthTokenResult extends PacketResult
{
    public PacketAuthTokenResult(byte[] nonce)
    {
       super(TdsDefinitions.TDS_AUTH_TOKEN);

       m_nonce = nonce;
    }

    public byte[] getNonce() { return m_nonce; }

    private byte[] m_nonce;
}

