# Microsoft Developer Studio Generated NMAKE File, Based on ntlmauth.dsp
!IF "$(CFG)" == ""
CFG=ntlmauth - Win32 Debug
!MESSAGE No configuration specified. Defaulting to ntlmauth - Win32 Debug.
!ENDIF 

!IF "$(CFG)" != "ntlmauth - Win32 Release" && "$(CFG)" != "ntlmauth - Win32 Debug"
!MESSAGE Invalid configuration "$(CFG)" specified.
!MESSAGE You can specify a configuration when running NMAKE
!MESSAGE by defining the macro CFG on the command line. For example:
!MESSAGE 
!MESSAGE NMAKE /f "ntlmauth.mak" CFG="ntlmauth - Win32 Debug"
!MESSAGE 
!MESSAGE Possible choices for configuration are:
!MESSAGE 
!MESSAGE "ntlmauth - Win32 Release" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE "ntlmauth - Win32 Debug" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE 
!ERROR An invalid configuration is specified.
!ENDIF 

!IF "$(OS)" == "Windows_NT"
NULL=
!ELSE 
NULL=nul
!ENDIF 

!IF  "$(CFG)" == "ntlmauth - Win32 Release"

!MESSAGE Generating ntlmauth.dll into .\Release 
OUTDIR=.\Release
INTDIR=.\Release
# Begin Custom Macros
OutDir=.\Release
# End Custom Macros

ALL : "$(OUTDIR)\ntlmauth.dll"


CLEAN :
	-@erase "$(INTDIR)\SspiClient.obj"
	-@erase "$(INTDIR)\SspiEx.obj"
	-@erase "$(INTDIR)\SSPIJNIClient.obj"
	-@erase "$(INTDIR)\SspiPackage.obj"
	-@erase "$(INTDIR)\vc60.idb"
	-@erase "$(OUTDIR)\ntlmauth.dll"
	-@erase "$(OUTDIR)\ntlmauth.exp"
	-@erase "$(OUTDIR)\ntlmauth.lib"

"$(OUTDIR)" :
    if not exist "$(OUTDIR)/$(NULL)" mkdir "$(OUTDIR)"

CPP=cl.exe
CPP_PROJ=/nologo /MT /W3 /GX /O2 /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "NTLMAUTH_EXPORTS" /Fp"$(INTDIR)\ntlmauth.pch" /YX /Fo"$(INTDIR)\\" /Fd"$(INTDIR)\\" /FD /c 

.c{$(INTDIR)}.obj::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cpp{$(INTDIR)}.obj::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cxx{$(INTDIR)}.obj::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.c{$(INTDIR)}.sbr::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cpp{$(INTDIR)}.sbr::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cxx{$(INTDIR)}.sbr::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

MTL=midl.exe
MTL_PROJ=/nologo /D "NDEBUG" /mktyplib203 /win32 
RSC=rc.exe
BSC32=bscmake.exe
BSC32_FLAGS=/nologo /o"$(OUTDIR)\ntlmauth.bsc" 
BSC32_SBRS= \
	
LINK32=link.exe
LINK32_FLAGS=kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /incremental:no /pdb:"$(OUTDIR)\ntlmauth.pdb" /machine:I386 /out:"$(OUTDIR)\ntlmauth.dll" /implib:"$(OUTDIR)\ntlmauth.lib" 
LINK32_OBJS= \
	"$(INTDIR)\SspiClient.obj" \
	"$(INTDIR)\SspiEx.obj" \
	"$(INTDIR)\SSPIJNIClient.obj" \
	"$(INTDIR)\SspiPackage.obj"

"$(OUTDIR)\ntlmauth.dll" : "$(OUTDIR)" $(DEF_FILE) $(LINK32_OBJS)
    $(LINK32) @<<
  $(LINK32_FLAGS) $(LINK32_OBJS)
<<

!ELSEIF  "$(CFG)" == "ntlmauth - Win32 Debug"
!MESSAGE Generating ntlmauth.dll into .\Debug
OUTDIR=.\Debug
INTDIR=.\Debug
# Begin Custom Macros
OutDir=.\Debug
# End Custom Macros

ALL : "$(OUTDIR)\ntlmauth.dll"


CLEAN :
	-@erase "$(INTDIR)\SspiClient.obj"
	-@erase "$(INTDIR)\SspiEx.obj"
	-@erase "$(INTDIR)\SSPIJNIClient.obj"
	-@erase "$(INTDIR)\SspiPackage.obj"
	-@erase "$(INTDIR)\vc60.idb"
	-@erase "$(INTDIR)\vc60.pdb"
	-@erase "$(OUTDIR)\ntlmauth.dll"
	-@erase "$(OUTDIR)\ntlmauth.exp"
	-@erase "$(OUTDIR)\ntlmauth.ilk"
	-@erase "$(OUTDIR)\ntlmauth.lib"
	-@erase "$(OUTDIR)\ntlmauth.pdb"

"$(OUTDIR)" :
    if not exist "$(OUTDIR)/$(NULL)" mkdir "$(OUTDIR)"

CPP=cl.exe
CPP_PROJ=/nologo /MTd /W3 /Gm /GX /ZI /Od /D "WIN32" /D "_DEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "NTLMAUTH_EXPORTS" /Fp"$(INTDIR)\ntlmauth.pch" /YX /Fo"$(INTDIR)\\" /Fd"$(INTDIR)\\" /FD /GZ /c 

.c{$(INTDIR)}.obj::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cpp{$(INTDIR)}.obj::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cxx{$(INTDIR)}.obj::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.c{$(INTDIR)}.sbr::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cpp{$(INTDIR)}.sbr::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

.cxx{$(INTDIR)}.sbr::
   $(CPP) @<<
   $(CPP_PROJ) $< 
<<

MTL=midl.exe
MTL_PROJ=/nologo /D "_DEBUG" /mktyplib203 /win32 
RSC=rc.exe
BSC32=bscmake.exe
BSC32_FLAGS=/nologo /o"$(OUTDIR)\ntlmauth.bsc" 
BSC32_SBRS= \
	
LINK32=link.exe
LINK32_FLAGS=kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /incremental:yes /pdb:"$(OUTDIR)\ntlmauth.pdb" /debug /machine:I386 /out:"$(OUTDIR)\ntlmauth.dll" /implib:"$(OUTDIR)\ntlmauth.lib" /pdbtype:sept 
LINK32_OBJS= \
	"$(INTDIR)\SspiClient.obj" \
	"$(INTDIR)\SspiEx.obj" \
	"$(INTDIR)\SSPIJNIClient.obj" \
	"$(INTDIR)\SspiPackage.obj"

"$(OUTDIR)\ntlmauth.dll" : "$(OUTDIR)" $(DEF_FILE) $(LINK32_OBJS)
    $(LINK32) @<<
  $(LINK32_FLAGS) $(LINK32_OBJS)
<<

!ENDIF 


!IF "$(NO_EXTERNAL_DEPS)" != "1"
!IF EXISTS("ntlmauth.dep")
!INCLUDE "ntlmauth.dep"
!ELSE 
!MESSAGE Warning: cannot find "ntlmauth.dep"
!ENDIF 
!ENDIF 


!IF "$(CFG)" == "ntlmauth - Win32 Release" || "$(CFG)" == "ntlmauth - Win32 Debug"
SOURCE=.\Src\SspiClient.cpp

"$(INTDIR)\SspiClient.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=.\Src\SspiEx.cpp

"$(INTDIR)\SspiEx.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=.\Src\SSPIJNIClient.cpp

"$(INTDIR)\SSPIJNIClient.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)


SOURCE=.\Src\SspiPackage.cpp

"$(INTDIR)\SspiPackage.obj" : $(SOURCE) "$(INTDIR)"
	$(CPP) $(CPP_PROJ) $(SOURCE)



!ENDIF 

