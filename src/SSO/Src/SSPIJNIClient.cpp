#include "StdAfx.h"

//Global variables

PSecurityFunctionTable  _pSecurityInterface = NULL;      // security interface table


HINSTANCE               _hProvider          = NULL;      // provider dll's instance

SspiClient*				client				= NULL;		//Ptr to the SSPI Client


JNIEXPORT void JNICALL Java_net_sourceforge_jtds_util_SSPIJNIClient_initialize
(JNIEnv * env, jobject obj) {

	_hProvider = LoadLibrary ( _T("secur32.dll") );
    if ( _hProvider == NULL ) {
        // secur32.dll not normally available on windows NT < 5.0
        _hProvider = LoadLibrary ( _T("security.dll") );
        if (_hProvider == NULL) {
            // Neither found so give up!
    		THROWEX ( ErrorNoLibrary );
        }
    }

	INIT_SECURITY_INTERFACE InitSecurityInterface;

    // Get the address of the InitSecurityInterface function.
	InitSecurityInterface = reinterpret_cast<INIT_SECURITY_INTERFACE> (
										      GetProcAddress (
													_hProvider,
													INIT_SEC_INTERFACE_NAME
												)
										 );
	if ( InitSecurityInterface == NULL )
		THROWEX ( ErrorNoSecurityInterface );

	_pSecurityInterface = InitSecurityInterface ( );
	if ( _pSecurityInterface == NULL )
		THROWEX ( ErrorNoSecurityInterface );
}

JNIEXPORT void JNICALL Java_net_sourceforge_jtds_util_SSPIJNIClient_unInitialize
(JNIEnv * env, jobject obj) {

	FreeLibrary ( _hProvider );
    _hProvider = NULL;
    _pSecurityInterface = NULL;
}

JNIEXPORT jbyteArray JNICALL Java_net_sourceforge_jtds_util_SSPIJNIClient_prepareSSORequest
(JNIEnv *env, jobject obj) {

	client = new SspiClient(_T("NTLM"), NULL );
    client->AcquireCredentials ();
    SspiData* ntlmMsg = client->PrepareOutboundPackage (NULL, 0);
	if (ntlmMsg->Size > 0) {
		jbyteArray retBuf = env->NewByteArray((unsigned long)ntlmMsg->Size);
		env->SetByteArrayRegion(retBuf, 0, (unsigned long)ntlmMsg->Size, (const signed char *)ntlmMsg->Buffer);
		return retBuf;
	} else {
		return NULL;
	}

}

JNIEXPORT jbyteArray JNICALL Java_net_sourceforge_jtds_util_SSPIJNIClient_prepareSSOSubmit
(JNIEnv *env, jobject obj, jbyteArray buf, jlong size)
{

	jbyte* newBuf = env->GetByteArrayElements(buf, NULL);
	SspiData* ntlmMsg = client->PrepareOutboundPackage (newBuf, size);
	if (ntlmMsg->Size > 0) {
		jbyteArray retBuf = env->NewByteArray(ntlmMsg->Size);
		env->SetByteArrayRegion(retBuf, 0, ntlmMsg->Size, (const signed char *)ntlmMsg->Buffer);
		delete client;
		return retBuf;
	} else {
		return NULL;
	}
}

