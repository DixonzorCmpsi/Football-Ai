<#
.SYNOPSIS
  Prints the first TCP port that is genuinely free, starting with the preferred one.

.DESCRIPTION
  Windows reserves blocks of ports at boot for Hyper-V / WinNAT (see
  `netsh interface ipv4 show excludedportrange protocol=tcp`), and the blocks
  move between boots. On 2026-09-12 the range 5249-5348 swallowed the frontend's
  5273, and Vite died with "listen EACCES: permission denied 0.0.0.0:5273".
  EACCES is not "in use", so Vite's own port fallback never kicked in.

  Exits 1 if nothing is free.

.EXAMPLE
  powershell -NoProfile -File scripts\find_free_port.ps1 -Preferred 5273
#>
param(
  [Parameter(Mandatory = $true)][int]$Preferred,
  # Where to look next. Away from 8000 (backend), 5432 (Postgres) and 3000, which
  # is Grafana on this machine and a common default for other dev servers.
  [int[]]$Fallbacks = @((5400..5999) + (3001..3099))
)

function Test-Free([int]$Port) {
  # 1. Anything already listening, on any address, rules the port out. A bind test
  #    alone is not enough: Windows let a test socket bind [::]:3000 while Docker's
  #    relay was listening there for Grafana.
  if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) {
    return $false
  }
  # 2. Then actually bind, exclusively, on both address families. This catches
  #    reserved ranges (the EACCES case), which have no listener to report. IPv6
  #    matters because the browser tries localhost as ::1 first, so a port free on
  #    IPv4 only would send it somewhere else.
  foreach ($address in @([System.Net.IPAddress]::Any, [System.Net.IPAddress]::IPv6Any)) {
    try {
      $listener = [System.Net.Sockets.TcpListener]::new($address, $Port)
      $listener.ExclusiveAddressUse = $true
      $listener.Start()
      $listener.Stop()
    } catch {
      return $false
    }
  }
  return $true
}

foreach ($candidate in @($Preferred) + $Fallbacks) {
  if (Test-Free $candidate) {
    Write-Output $candidate
    exit 0
  }
}
exit 1
