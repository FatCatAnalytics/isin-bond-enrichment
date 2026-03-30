; ISIN Bond Enrichment - Inno Setup Installer Script
; Built automatically via GitHub Actions on a Windows machine
; No need to install Inno Setup locally

[Setup]
AppName=ISIN Bond Enrichment
AppVersion=1.0.0
AppPublisher=Your Company Name
AppPublisherURL=https://yourwebsite.com
DefaultDirName={userdocs}\ISIN Bond Enrichment
DefaultGroupName=ISIN Bond Enrichment
OutputBaseFilename=ISIN_Bond_Enrichment_Setup
Compression=lzma2/ultra64
SolidCompression=yes
PrivilegesRequired=lowest
UninstallDisplayName=ISIN Bond Enrichment
WizardStyle=modern
DisableProgramGroupPage=yes
AppCopyright=Copyright (c) 2026

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Files]
; Application files
Source: "server.py"; DestDir: "{app}"; Flags: ignoreversion
Source: "bond_enhancement.py"; DestDir: "{app}"; Flags: ignoreversion
Source: "dashboard.html"; DestDir: "{app}"; Flags: ignoreversion
Source: "requirements.txt"; DestDir: "{app}"; Flags: ignoreversion
Source: "start.bat"; DestDir: "{app}"; Flags: ignoreversion
Source: "start.sh"; DestDir: "{app}"; Flags: ignoreversion
Source: "ISIN Template.xlsx"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
; Desktop shortcut
Name: "{userdesktop}\ISIN Bond Enrichment"; Filename: "{app}\start.bat"; WorkingDir: "{app}"; Comment: "Launch ISIN Bond Enrichment Terminal"

; Start Menu shortcuts
Name: "{group}\ISIN Bond Enrichment"; Filename: "{app}\start.bat"; WorkingDir: "{app}"
Name: "{group}\Uninstall ISIN Bond Enrichment"; Filename: "{uninstallexe}"

[Run]
; Option to launch app after install
Filename: "{app}\start.bat"; Description: "Launch ISIN Bond Enrichment now"; WorkingDir: "{app}"; Flags: nowait postinstall skipifsilent shellexec

[UninstallDelete]
; Clean up venv and cache on uninstall
Type: filesandordirs; Name: "{app}\.venv"
Type: filesandordirs; Name: "{app}\__pycache__"
