#pragma once

#define single_site_db_wrapper_t_types \
    template <bool enable_db>
#define single_site_db_wrapper_t_templ single_site_db_wrapper_t<enable_db>

#define single_site_db_types \
    template <bool enable_db>
#define single_site_db_templ single_site_db<enable_db>
