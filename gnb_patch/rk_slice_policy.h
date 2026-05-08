#ifndef RK_SLICE_POLICY_H
#define RK_SLICE_POLICY_H

#include <stdint.h>

#define RK_SLICE_ID_NONE         0
#define RK_SLICE_ID_BACKGROUND   1
#define RK_SLICE_ID_BALANCED     2
#define RK_SLICE_ID_THROUGHPUT   3
#define RK_SLICE_ID_LATENCY      4
#define RK_SLICE_ID_UPLINK_BOOST 5
#define RK_SLICE_ID_CAPPED_FAIR  6

#define RK_SLICE_ASSOC_MAX 256

typedef struct {
  uint16_t rnti;
  uint16_t dl_id;
  uint16_t ul_id;
  uint8_t valid;
} rk_slice_assoc_entry_t;

typedef struct {
  int   slice_id;
  float dl_weight_mul;
  float ul_weight_mul;
  int   dl_rb_cap;
  int   ul_rb_cap;

  int   dl_rb_floor;
  int   ul_rb_floor;

  int   dl_max_consecutive_grants;
  int   ul_max_consecutive_grants;
} rk_slice_policy_t;

extern rk_slice_assoc_entry_t g_rk_slice_assoc[RK_SLICE_ASSOC_MAX];

void rk_slice_assoc_update_by_rnti(uint16_t rnti, int dl_id, int ul_id);

int rk_slice_assoc_get_dl_id(uint16_t rnti);
int rk_slice_assoc_get_ul_id(uint16_t rnti);

const rk_slice_policy_t *rk_slice_policy_get(int slice_id);

float rk_slice_assoc_get_dl_weight(uint16_t rnti);
float rk_slice_assoc_get_ul_weight(uint16_t rnti);

int rk_slice_assoc_get_dl_rb_cap(uint16_t rnti);
int rk_slice_assoc_get_ul_rb_cap(uint16_t rnti);

int rk_slice_assoc_get_dl_rb_floor(uint16_t rnti);
int rk_slice_assoc_get_ul_rb_floor(uint16_t rnti);

int rk_slice_assoc_get_dl_max_consecutive_grants(uint16_t rnti);
int rk_slice_assoc_get_ul_max_consecutive_grants(uint16_t rnti);

int  rk_slice_runtime_dl_should_throttle(uint16_t rnti, int now_tick, int max_consecutive_grants);
int  rk_slice_runtime_ul_should_throttle(uint16_t rnti, int now_tick, int max_consecutive_grants);
void rk_slice_runtime_note_dl_grant(uint16_t rnti, int now_tick);
void rk_slice_runtime_note_ul_grant(uint16_t rnti, int now_tick);

#endif
