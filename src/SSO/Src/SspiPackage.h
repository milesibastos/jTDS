//==============================================================================
// File:		SspiPackage.h
//
// Description:	header file for the SspiPackage and related classes.
//
//
// Classes:     - SspiPackage is a lightweight wrapper around SecPkgInfo.
//              - SspiPackageList is a lightweight class that handles package 
//                enumeration, and simple iteration
//==============================================================================

#if !defined(_SSPIPACKAGE_H_)
#define _SSPIPACKAGE_H_

   // SspiPackage declaration
    class SspiPackage
    {
    public:
        SspiPackage ( const TCHAR * PackageName = NULL );
        SspiPackage ( const SecPkgInfo * package );
        ~SspiPackage ( );

        // -- copy constructor and assigment --
        SspiPackage ( const SspiPackage & package );
        const SspiPackage & operator= ( const SspiPackage & package );
        const SspiPackage & operator= ( const SecPkgInfo * package );
        
        // -- accessors --
        const SecPkgInfo * operator-> ( ) const;
        bool HasCapabilities ( ULONG Capabilities ) const;

    private:
        // -- private methods --
        bool GetLocalPkgCopy ( const SecPkgInfo * package );
    private:
        bool          m_HavePkg;
        // -- package info --
        SecPkgInfo    m_PkgInfo;
        // -- clean up --
        void FreePkgInfo ( );
    };

    // -- package dumper --
//    wsspi_ostream & operator<< ( wsspi_ostream & o, const SspiPackage & p );

    // SspiPackageList declaration
    class SspiPackageList
    {
    private:
        typedef std::vector<SspiPackage> PkgVector;
    public:
        typedef PkgVector::iterator iterator;
        typedef PkgVector::const_iterator const_iterator;

        SspiPackageList ( );
        virtual ~SspiPackageList ( );

        // -- traversing operators --
        const SspiPackage & operator[] ( size_t pos );
        iterator begin ( ) 
        { return m_Packages.begin ( ); }
        const_iterator begin ( ) const
        { return m_Packages.begin ( ); }
        iterator end ( ) 
        { return m_Packages.end( );    }
        const_iterator end ( ) const
        { return m_Packages.end( );    }

        // -- hide copy constructors and assignment --
    private:
        SspiPackageList ( const SspiPackage& Package );
        const SspiPackage operator= ( const SspiPackage & Package );

        // -- protected data --
    protected:
        // package list
        PkgVector       m_Packages; 
    };


#endif // _SSPIPACKAGE_H