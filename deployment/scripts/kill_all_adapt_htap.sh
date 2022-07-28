pxkill() {
  grepFor="$1"

  sudo ps aux | grep ${grepFor} | grep -v grep | awk '{print $2}' | xargs -I {} sudo kill -9 {}
}

pxkill "site_selector_server"
pxkill "site_manager_server"
pxkill "oltpbench"
