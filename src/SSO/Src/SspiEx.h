//==============================================================================
// File:		SspiEx.h
//
// Description:	definition of exceptions
//
// Revisions: 	Created: 11/20/1999
//
//==============================================================================
// Copyright(C) 1999, Tomas Restrepo. All rights reserved
//==============================================================================


#if !defined(_SSPIEX_H_)
#define _SSPIEX_H_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

typedef std::wstring  wsspi_string;  
typedef std::wostream wsspi_ostream;

    enum SspiError
    {
        ErrorSuccess = 0,                   // no errors
        ErrorNotInitialized,                // SspiInitialize() was not called
        ErrorNoLibrary,                     // provider dll was not found
        ErrorNoPackage,                     // no package has been found
        ErrorPackageEnumerationFailed,      // EnumerateSecurityPackages() failed
        ErrorNoSecurityInterface,           // failed to setup security interface
        ErrorNoCredentials,                 // no credentials could be obtained
        ErrorNoMemory,                      // not enough memory to complete the operation
        ErrorNullPointer,                   // an invalid null pointer was passed
        ErrorAuthFailed,                    // authentication failed
        ErrorImpersonationFailed,           // impersonation failed
        ErrorRevertToSelfFailed,            // failed to revert to original security context
        ErrorInvalidArrayIndex,             // array index was out of bounds
        ErrorUnknown                        // unknown error
    };

    class SspiException
    {
    public:
        SspiException ( TCHAR*  File, int LineNumber, 
                        SspiError ErrorCode = ErrorSuccess,
                        SECURITY_STATUS Status = SEC_E_OK );
        ~SspiException ( ) { }

        // -- copy constructor and assignment --
        SspiException ( const SspiException& e );
        const SspiException& operator= ( const SspiException& e );

        // -- accessors --
        TCHAR* GetFileName ( ) const		   { return m_FileName;   }
        int GetLineNumber ( ) const            { return m_LineNumber; }
        SspiError GetError ( ) const           { return m_ErrorCode;  }
        SECURITY_STATUS GetSspiError ( ) const { return m_SspiStatus; }

        void Release ( );
        
    private:
        TCHAR*			m_FileName;
        int             m_LineNumber;
        SspiError       m_ErrorCode;
        SECURITY_STATUS m_SspiStatus;
    };

    
#define THROWEX(error) (throw new SspiException ( _T(__FILE__), __LINE__, (error) ))
#define THROWEXE(error, hr)(throw new SspiException ( _T(__FILE__), __LINE__, (error), (hr) ))
#endif // _SSPIEX_H_
