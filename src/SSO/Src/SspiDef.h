//==============================================================================
// File:		SspiDef.h
//
// Description:	some common definitions needed by SspiServer and SspiClient
//==============================================================================

#if !defined(_SSPIDEF_H_)
#define _SSPIDEF_H_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

   // wsspi data that gets interchanged between
    // client and server
    typedef struct SspiData
    {
        DWORD    Size;        // size of buffer
        BYTE     Buffer[1];   // real buffer
    } SspiData;
    // NOTE: The space allocated for Buffer 
    // might be larger than SspiData::Size

    // enumerations
    enum AuthState
    {
        AuthSuccess =0,     // authentication successful
        AuthFailed,         // authentication failed (denied)
        AuthContinue,       // keep on going
        AuthError           // an error occurred during authentication
    };

#endif // _SSPIDEF_H_