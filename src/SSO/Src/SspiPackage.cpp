//==============================================================================
// File:		SspiPackage.cpp
//
// Description:	Implementation file for the SspiPackage class
//
//==============================================================================

#include "StdAfx.h"

extern PSecurityFunctionTable  _pSecurityInterface;      // security interface table

//==============================================================================
// SspiPackage
//==============================================================================

////////////////////////////////////////////////////////////////////////////////
// construction/destruction
////////////////////////////////////////////////////////////////////////////////
SspiPackage::SspiPackage ( const TCHAR * PackageName /* = NULL */ )
   : m_HavePkg ( false )
{
   // make sure library is initialized
   //InitLib ( );
   
   SECURITY_STATUS status;
   
   if ( PackageName != NULL )
   {
      SecPkgInfo * pPackage = NULL;
      status = _pSecurityInterface->QuerySecurityPackageInfo ( 
         const_cast<TCHAR*>(PackageName),
         &pPackage
         );
      if ( status != SEC_E_OK )
         THROWEXE ( ErrorNoPackage, status );
      if ( !GetLocalPkgCopy ( pPackage ) )
         THROWEX ( ErrorNoPackage );
      if ( _pSecurityInterface->FreeContextBuffer != NULL )
         _pSecurityInterface->FreeContextBuffer ( (void*) pPackage );
   }
}

SspiPackage::SspiPackage ( const SecPkgInfo * package )
   : m_HavePkg ( false )
{
   // make sure library is initialized
   //InitLib ( );
   if ( !GetLocalPkgCopy ( package ) )
      THROWEX ( ErrorNoPackage );
}

SspiPackage::~SspiPackage ( )
{
   FreePkgInfo ( );
}

void SspiPackage::FreePkgInfo ( )
{
   if ( m_HavePkg )
   {
      free ( m_PkgInfo.Name );
      free ( m_PkgInfo.Comment );
   }
}

//==========================================================
// SspiPackage::GetLocalPkgCopy()
//	Description
//		we allocate our own copy of the package
//      using new
//	Parameters
//		package - a package info struct
//  Return
//      void - none
//==========================================================
bool SspiPackage::GetLocalPkgCopy ( const SecPkgInfo * package )
{
   memcpy ( (void*)&m_PkgInfo, (void*)package, sizeof SecPkgInfo );
   m_PkgInfo.Name = _tcsdup ( package->Name );
   if ( m_PkgInfo.Name != 0 )
   {
      m_PkgInfo.Comment = _tcsdup ( package->Comment );
      if ( m_PkgInfo.Comment != 0 )
      {
         _tcscpy ( m_PkgInfo.Comment, package->Comment );
         m_HavePkg = true;
         return true;
      }
      free ( m_PkgInfo.Name );
   }
   m_HavePkg = false;
   return false;
}

////////////////////////////////////////////////////////////////////////////////
// copy construction/assignment
////////////////////////////////////////////////////////////////////////////////
SspiPackage::SspiPackage ( const SspiPackage & package )
{
   if ( !package.m_HavePkg || !GetLocalPkgCopy ( &package.m_PkgInfo ) )
      THROWEX ( ErrorNoPackage );
}

const SspiPackage & SspiPackage::operator= ( const SspiPackage & package )
{
   if ( this != &package )
   {
      if ( !GetLocalPkgCopy ( &package.m_PkgInfo ) )
         THROWEX ( ErrorNoPackage );
   }
   
   return *this;
}

const SspiPackage & SspiPackage::operator= ( const SecPkgInfo* package )
{
   if ( !GetLocalPkgCopy ( package ) )
      THROWEX ( ErrorNoPackage );
   return *this;
}

////////////////////////////////////////////////////////////////////////////////
// interface
////////////////////////////////////////////////////////////////////////////////
//==========================================================
// SspiPackage::operator->()
//	Description
//		forwards calls to SecPkgInfo(), instead of 
//      providing accessors for all members
//	Parameters
//	Return
//		SecPkgInfo* - a package info struct
//==========================================================
const SecPkgInfo * SspiPackage::operator-> ( ) const
{
   if ( !m_HavePkg )
      THROWEX ( ErrorNoPackage );
   return &m_PkgInfo;
}

//==========================================================
// SspiPackage::HasCapabilities()
//	Description
//		tests if the provider meets the requested caps
//	Parameters
//		Capabilities - mask of provider cpas
//	Return
//		bool		 - true if it has the caps required
//==========================================================
bool SspiPackage::HasCapabilities ( ULONG Capabilities ) const
{
   return ((m_PkgInfo.fCapabilities & Capabilities) == Capabilities );
}


//======================== Package Dumper ===================
namespace
{
   // capabilities flags
   struct 
   {
      DWORD         cap;        // capability
      const TCHAR*  comment;    // name and comment
   } Caps[] =
   {
      { SECPKG_FLAG_INTEGRITY,         _T("SECPKG_FLAG_INTEGRITY: Supports message integrity.") },
      { SECPKG_FLAG_PRIVACY,           _T("SECPKG_FLAG_PRIVACY: Supports message encription.") },
      { SECPKG_FLAG_TOKEN_ONLY,        _T("SECPKG_FLAG_TOKEN_ONLY: Only supports SECBUFFER_TOKEN buffers.") },
      { SECPKG_FLAG_DATAGRAM,          _T("SECPKG_FLAG_DATAGRAM: Supports datagram-style authentication.") },
      { SECPKG_FLAG_CONNECTION,        _T("SECPKG_FLAG_CONNECTION: Supports connection-oriented style authentication.") },
      { SECPKG_FLAG_MULTI_REQUIRED,    _T("SECPKG_FLAG_MULTI_REQUIRED: Multiple legs are required for authentication.") },
      { SECPKG_FLAG_CLIENT_ONLY,       _T("SECPKG_FLAG_CLIENT_ONLY: Server authentication support is not provided.") },
      { SECPKG_FLAG_EXTENDED_ERROR,    _T("SECPKG_FLAG_EXTENDED_ERROR:  Supports extended error handling.") },
      { SECPKG_FLAG_IMPERSONATION,     _T("SECPKG_FLAG_IMPERSONATION: Supports Win32 impersonation in server contexts.") },
      { SECPKG_FLAG_ACCEPT_WIN32_NAME, _T("SECPKG_FLAG_ACCEPT_WIN32_NAME: Understands Win32 principal and target names.") },
      { SECPKG_FLAG_STREAM,            _T("SECPKG_FLAG_STREAM: Supports stream semantics.") },
      { 0xFFFFFFFF,                    _T("") }
   };
} // namespace

namespace wsspi
{
   wsspi_ostream& operator<< ( wsspi_ostream& o, const SspiPackage& p )
   {
      
      o << _T("Package: ") << p->Name << std::endl
         << _T("Description: ") << p->Comment << std::endl
         << _T("Capabilities: ") << std::endl;
      
      for ( int i = 0; Caps[i].cap != 0xFFFFFFFF; ++i )
      {
         if ( p.HasCapabilities ( Caps[i].cap ) )
            o << Caps[i].comment << std::endl;
      }
      return o;
   }
} // namespace wsspi

//==============================================================================
// SspiPackage
//==============================================================================

////////////////////////////////////////////////////////////////////////////////
// construction/destruction
////////////////////////////////////////////////////////////////////////////////
SspiPackageList::SspiPackageList ( )
{
   // make sure library is initialized
   //InitLib ( );
   
   DWORD           index = 0;
   SECURITY_STATUS status;
   SecPkgInfo *    packages = 0;
   ULONG           numpkgs = 0;
   
   if ( _pSecurityInterface->EnumerateSecurityPackages == NULL )
      THROWEX ( ErrorNoSecurityInterface );
   
   status = _pSecurityInterface->EnumerateSecurityPackages (
      &numpkgs,
      &packages
      );
   if ( status != SEC_E_OK )
      THROWEXE ( ErrorPackageEnumerationFailed, status );
   
   // build the list
   for ( ULONG i = 0; i < numpkgs; i++ )
      m_Packages.push_back ( &packages[i] );
   
   if ( _pSecurityInterface->FreeContextBuffer != NULL )
      _pSecurityInterface->FreeContextBuffer ( (void*)packages );
}

SspiPackageList::~SspiPackageList ( )
{
}

//==========================================================
// operator[]()
//	Description
//		handles iteration of the list
//
//	Parameters
//		pos		    - position of entry in the list
//	Return
//		SspiPackage - package at that position
//==========================================================

const SspiPackage & SspiPackageList::operator[] ( size_t pos )
{
   if ( pos >= m_Packages.size ( ) )
      THROWEX ( ErrorInvalidArrayIndex );
   
   return m_Packages[pos];
}
