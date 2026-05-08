#ifndef RK_UE_DL_POLICY_H
#define RK_UE_DL_POLICY_H

#include <stdint.h>

void rk_ue_dl_reload_if_needed(const char *path);
int rk_ue_dl_get_max_mcs(uint16_t rnti, int default_val);
int rk_ue_dl_get_min_grant_prb(uint16_t rnti, int default_val);
float rk_ue_dl_get_sched_mul(uint16_t rnti, float default_val);
int rk_ue_dl_get_maxcg_override(uint16_t rnti, int default_val);
int rk_ue_dl_get_small_burst_bytes(uint16_t rnti, int default_val);
float rk_ue_dl_get_small_burst_mul(uint16_t rnti, float default_val);

#endif /* RK_UE_DL_POLICY_H */
