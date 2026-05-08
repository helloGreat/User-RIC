#include "rk_slice_policy.h"

#include "common/utils/LOG/log.h"
#include <stddef.h>

rk_slice_assoc_entry_t g_rk_slice_assoc[RK_SLICE_ASSOC_MAX];

#define RK_SLICE_RUNTIME_MAX 256

typedef struct {
  uint16_t rnti;
  uint8_t  valid;

  int last_dl_tick;
  int last_ul_tick;

  int dl_consecutive_grants;
  int ul_consecutive_grants;
} rk_slice_runtime_entry_t;

static rk_slice_runtime_entry_t g_rk_slice_runtime[RK_SLICE_RUNTIME_MAX];

static const rk_slice_policy_t g_rk_slice_policy_table[] = {
  {0,  1.0f,  1.0f,  0,  0,  0,  0, 0, 0},   /* none/default */

  {1,  0.5f,  0.5f,  6,  6,  2,  2, 1, 1},   /* background */
  {2,  1.0f,  1.0f, 12, 12,  4,  4, 4, 4},   /* balanced */
  {3, 10.0f,  4.0f, 50, 30, 12, 12, 0, 0},   /* throughput */
  {4,  6.0f,  6.0f, 10, 10,  4,  4, 6, 6},   /* latency */
  {5,  1.0f, 10.0f, 12, 50,  4, 12, 2, 0},   /* uplink_boost */
  {6,  1.2f,  1.2f,  8,  8,  2,  2, 2, 2},   /* capped_fair */
};

static rk_slice_assoc_entry_t *rk_find_slice_assoc(uint16_t rnti, int create)
{
  for (int i = 0; i < RK_SLICE_ASSOC_MAX; ++i) {
    if (g_rk_slice_assoc[i].valid && g_rk_slice_assoc[i].rnti == rnti)
      return &g_rk_slice_assoc[i];
  }

  if (!create)
    return NULL;

  for (int i = 0; i < RK_SLICE_ASSOC_MAX; ++i) {
    if (!g_rk_slice_assoc[i].valid) {
      g_rk_slice_assoc[i].valid = 1;
      g_rk_slice_assoc[i].rnti = rnti;
      g_rk_slice_assoc[i].dl_id = 0;
      g_rk_slice_assoc[i].ul_id = 0;
      return &g_rk_slice_assoc[i];
    }
  }

  return NULL;
}

void rk_slice_assoc_update_by_rnti(uint16_t rnti, int dl_id, int ul_id)
{
  rk_slice_assoc_entry_t *e = rk_find_slice_assoc(rnti, 1);
  if (!e) {
    LOG_W(NR_MAC, "RK-SLICE no free assoc slot for UE %04x\n", rnti);
    return;
  }

  e->dl_id = dl_id;
  e->ul_id = ul_id;

  LOG_I(NR_MAC, "RK-SLICE ASSOC UE %04x -> dl=%d ul=%d\n", rnti, dl_id, ul_id);
}

int rk_slice_assoc_get_dl_id(uint16_t rnti)
{
  rk_slice_assoc_entry_t *e = rk_find_slice_assoc(rnti, 0);
  if (!e)
    return RK_SLICE_ID_NONE;
  return e->dl_id;
}

int rk_slice_assoc_get_ul_id(uint16_t rnti)
{
  rk_slice_assoc_entry_t *e = rk_find_slice_assoc(rnti, 0);
  if (!e)
    return RK_SLICE_ID_NONE;
  return e->ul_id;
}

static rk_slice_runtime_entry_t *rk_find_slice_runtime(uint16_t rnti, int create)
{
  for (int i = 0; i < RK_SLICE_RUNTIME_MAX; ++i) {
    if (g_rk_slice_runtime[i].valid && g_rk_slice_runtime[i].rnti == rnti)
      return &g_rk_slice_runtime[i];
  }

  if (!create)
    return NULL;

  for (int i = 0; i < RK_SLICE_RUNTIME_MAX; ++i) {
    if (!g_rk_slice_runtime[i].valid) {
      g_rk_slice_runtime[i].valid = 1;
      g_rk_slice_runtime[i].rnti = rnti;
      g_rk_slice_runtime[i].last_dl_tick = -1;
      g_rk_slice_runtime[i].last_ul_tick = -1;
      g_rk_slice_runtime[i].dl_consecutive_grants = 0;
      g_rk_slice_runtime[i].ul_consecutive_grants = 0;
      return &g_rk_slice_runtime[i];
    }
  }

  return NULL;
}

int rk_slice_runtime_dl_should_throttle(uint16_t rnti, int now_tick, int max_consecutive_grants)
{
  if (max_consecutive_grants <= 0)
    return 0;

  rk_slice_runtime_entry_t *e = rk_find_slice_runtime(rnti, 0);
  if (!e)
    return 0;

  if (e->last_dl_tick == (now_tick - 1) &&
      e->dl_consecutive_grants >= max_consecutive_grants)
    return 1;

  return 0;
}

int rk_slice_runtime_ul_should_throttle(uint16_t rnti, int now_tick, int max_consecutive_grants)
{
  if (max_consecutive_grants <= 0)
    return 0;

  rk_slice_runtime_entry_t *e = rk_find_slice_runtime(rnti, 0);
  if (!e)
    return 0;

  if (e->last_ul_tick == (now_tick - 1) &&
      e->ul_consecutive_grants >= max_consecutive_grants)
    return 1;

  return 0;
}

void rk_slice_runtime_note_dl_grant(uint16_t rnti, int now_tick)
{
  rk_slice_runtime_entry_t *e = rk_find_slice_runtime(rnti, 1);
  if (!e)
    return;

  if (e->last_dl_tick == (now_tick - 1))
    e->dl_consecutive_grants += 1;
  else
    e->dl_consecutive_grants = 1;

  e->last_dl_tick = now_tick;
}

void rk_slice_runtime_note_ul_grant(uint16_t rnti, int now_tick)
{
  rk_slice_runtime_entry_t *e = rk_find_slice_runtime(rnti, 1);
  if (!e)
    return;

  if (e->last_ul_tick == (now_tick - 1))
    e->ul_consecutive_grants += 1;
  else
    e->ul_consecutive_grants = 1;

  e->last_ul_tick = now_tick;
}

const rk_slice_policy_t *rk_slice_policy_get(int slice_id)
{
  const size_t n = sizeof(g_rk_slice_policy_table) / sizeof(g_rk_slice_policy_table[0]);

  if (slice_id < 0 || (size_t)slice_id >= n)
    return &g_rk_slice_policy_table[0];

  if (g_rk_slice_policy_table[slice_id].slice_id != slice_id)
    return &g_rk_slice_policy_table[0];

  return &g_rk_slice_policy_table[slice_id];
}

float rk_slice_assoc_get_dl_weight(uint16_t rnti)
{
  const int dl_id = rk_slice_assoc_get_dl_id(rnti);
  return rk_slice_policy_get(dl_id)->dl_weight_mul;
}

float rk_slice_assoc_get_ul_weight(uint16_t rnti)
{
  const int ul_id = rk_slice_assoc_get_ul_id(rnti);
  return rk_slice_policy_get(ul_id)->ul_weight_mul;
}

int rk_slice_assoc_get_dl_rb_cap(uint16_t rnti)
{
  const int dl_id = rk_slice_assoc_get_dl_id(rnti);
  return rk_slice_policy_get(dl_id)->dl_rb_cap;
}

int rk_slice_assoc_get_ul_rb_cap(uint16_t rnti)
{
  const int ul_id = rk_slice_assoc_get_ul_id(rnti);
  return rk_slice_policy_get(ul_id)->ul_rb_cap;
}

int rk_slice_assoc_get_dl_rb_floor(uint16_t rnti)
{
  const int dl_id = rk_slice_assoc_get_dl_id(rnti);
  return rk_slice_policy_get(dl_id)->dl_rb_floor;
}

int rk_slice_assoc_get_ul_rb_floor(uint16_t rnti)
{
  const int ul_id = rk_slice_assoc_get_ul_id(rnti);
  return rk_slice_policy_get(ul_id)->ul_rb_floor;
}

int rk_slice_assoc_get_dl_max_consecutive_grants(uint16_t rnti)
{
  const int dl_id = rk_slice_assoc_get_dl_id(rnti);
  return rk_slice_policy_get(dl_id)->dl_max_consecutive_grants;
}

int rk_slice_assoc_get_ul_max_consecutive_grants(uint16_t rnti)
{
  const int ul_id = rk_slice_assoc_get_ul_id(rnti);
  return rk_slice_policy_get(ul_id)->ul_max_consecutive_grants;
}
