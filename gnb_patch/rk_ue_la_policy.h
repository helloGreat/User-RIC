#ifndef RK_UE_LA_POLICY_H
#define RK_UE_LA_POLICY_H

#include <stdint.h>

#define RK_UE_LA_MAX 256
#define RK_UE_LA_UNSET (-1)

typedef struct {
  uint16_t rnti;
  uint8_t valid;

  int ul_max_mcs;
  int min_grant_prb;
  int ulsch_max_frame_inactivity;
  int pusch_target_snrx10;
  float ul_sched_mul;
  int ul_maxcg_override;
  int ul_small_burst_bytes;
  float ul_small_burst_mul;
} rk_ue_la_entry_t;

void rk_ue_la_reload_if_needed(const char *path);
void rk_ue_la_clear_all(void);

const rk_ue_la_entry_t *rk_ue_la_get_entry(uint16_t rnti);

int rk_ue_la_get_ul_max_mcs(uint16_t rnti, int default_v, int max_mcs_table);
int rk_ue_la_get_min_grant_prb(uint16_t rnti, int default_v);
int rk_ue_la_get_ulsch_max_frame_inactivity(uint16_t rnti, int default_v);
int rk_ue_la_get_pusch_target_snrx10(uint16_t rnti, int default_v);
float rk_ue_la_get_ul_sched_mul(uint16_t rnti, float default_v);
int rk_ue_la_get_ul_maxcg_override(uint16_t rnti, int default_v);
int rk_ue_la_get_ul_small_burst_bytes(uint16_t rnti, int default_v);
float rk_ue_la_get_ul_small_burst_mul(uint16_t rnti, float default_v);

#endif
