//==============================================================================
// File:		SspiEx.cpp
//
// Description:	implementation file for the SspiException Class
//
// Revisions: 	11/20/1999 - created
//              04/08/2000 - modified the dumper to get the Win32 error msg
//
//==============================================================================
// Copyright(C) 1999, Tomas Restrepo. All rights reserved
//==============================================================================
#include "StdAfx.h"
#include <sstream>

    const TCHAR *ErrorCodeStrings[] =
    {
        _T("ErrorSuccess: no errors"),
        _T("ErrorNotInitialized: SspiInitialize() was not called"),
        _T("ErrorNoLibrary: provider dll was not found"),
        _T("ErrorNoPackage: no package was found"),
        _T("ErrorPackageEnumerationFailed: EnumerateSecurityPackages() failed"),
        _T("ErrorNoSecurityInterface: failed to setup security interface"),
        _T("ErrorNoCredentials: no credentials could be obtained"),
        _T("ErrorNoMemory: not enough memory to complete the operation"),
        _T("ErrorNullPointer: an invalid null pointer was passed"),
        _T("ErrorAuthFailed: authentication failed"),
        _T("ErrorImpersonationFailed: impersonation failed"),
        _T("ErrorRevertToSelfFailed: failed to revert to original security context"),
        _T("ErrorInvalidArrayIndex: array index was out of bounds"),
        _T("ErrorUnknown: unknown error")
    };

	wsspi_ostream& operator<< ( wsspi_ostream& o, const SspiException& e )
    {
        LPTSTR pBuf = NULL;

        FormatMessage ( 
            FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
            NULL, e.GetSspiError ( ), GetUserDefaultLangID( ),
            (LPTSTR)&pBuf, 0, NULL
        );
        if ( pBuf != NULL ) 
        {
            o << e.GetFileName() << _T("(") << e.GetLineNumber() << _T("):\n") 
              << ErrorCodeStrings[e.GetError()] << std::endl 
              << _T("Sspi Error Code: ") << std::hex << e.GetSspiError() << std::endl
              << "\t" << pBuf << std::endl;

            LocalFree ( pBuf );
        }
        return o;
    }

/////////////////////////////////////////////////////////////////////////////////
// construction
/////////////////////////////////////////////////////////////////////////////////
SspiException::SspiException ( 
                    TCHAR*    File, 
                    int             LineNumber,
                    SspiError       ErrorCode,  /* = ErrorSuccess */
                    SECURITY_STATUS Status      /* = SEC_E_OK */
                )
{
	m_FileName = File;
	m_LineNumber= LineNumber;
	m_ErrorCode = ErrorCode;
	m_SspiStatus = Status;
    if ( m_ErrorCode > ErrorUnknown )
        m_ErrorCode = ErrorUnknown;
}

/////////////////////////////////////////////////////////////////////////////////
// copy constructor and assignment
/////////////////////////////////////////////////////////////////////////////////
SspiException::SspiException ( const SspiException& e )
{
    m_FileName    = e.m_FileName;
    m_LineNumber  = e.m_LineNumber;
    m_ErrorCode   = e.m_ErrorCode;
    m_SspiStatus  = e.m_SspiStatus;
}

const SspiException& SspiException::operator= ( const SspiException& e )
{
    if ( this != &e )
    {
        m_FileName    = e.m_FileName;
        m_LineNumber  = e.m_LineNumber;
        m_ErrorCode   = e.m_ErrorCode;
        m_SspiStatus  = e.m_SspiStatus;
    }
    return *this;
}

void SspiException::Release ( )
{
    delete this;
}