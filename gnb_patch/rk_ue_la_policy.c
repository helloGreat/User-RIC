#include "rk_ue_la_policy.h"

#include "common/utils/LOG/log.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

static rk_ue_la_entry_t g_tbl[RK_UE_LA_MAX];
static time_t g_last_mtime_sec = 0;
static long g_last_mtime_nsec = 0;
static long long g_last_check_ms = 0;

static long long rk_now_ms(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

void rk_ue_la_clear_all(void)
{
  memset(g_tbl, 0, sizeof(g_tbl));
}

static rk_ue_la_entry_t *rk_ue_la_find_mut(uint16_t rnti)
{
  for (int i = 0; i < RK_UE_LA_MAX; ++i) {
    if (g_tbl[i].valid && g_tbl[i].rnti == rnti)
      return &g_tbl[i];
  }
  return NULL;
}

static rk_ue_la_entry_t *rk_ue_la_alloc(uint16_t rnti)
{
  rk_ue_la_entry_t *e = rk_ue_la_find_mut(rnti);
  if (e)
    return e;

  for (int i = 0; i < RK_UE_LA_MAX; ++i) {
    if (!g_tbl[i].valid) {
      g_tbl[i].valid = 1;
      g_tbl[i].rnti = rnti;
      g_tbl[i].ul_max_mcs = RK_UE_LA_UNSET;
      g_tbl[i].min_grant_prb = RK_UE_LA_UNSET;
      g_tbl[i].ulsch_max_frame_inactivity = RK_UE_LA_UNSET;
      g_tbl[i].pusch_target_snrx10 = RK_UE_LA_UNSET;
      g_tbl[i].ul_sched_mul = -1.0f;
      g_tbl[i].ul_maxcg_override = RK_UE_LA_UNSET;
      g_tbl[i].ul_small_burst_bytes = RK_UE_LA_UNSET;
      g_tbl[i].ul_small_burst_mul = -1.0f;
      return &g_tbl[i];
    }
  }
  return NULL;
}

const rk_ue_la_entry_t *rk_ue_la_get_entry(uint16_t rnti)
{
  for (int i = 0; i < RK_UE_LA_MAX; ++i) {
    if (g_tbl[i].valid && g_tbl[i].rnti == rnti)
      return &g_tbl[i];
  }
  return NULL;
}

static int parse_int_kv(const char *tok, const char *key, int *out)
{
  size_t n = strlen(key);
  if (strncmp(tok, key, n) != 0 || tok[n] != '=')
    return 0;
  *out = atoi(tok + n + 1);
  return 1;
}

void rk_ue_la_reload_if_needed(const char *path)
{
  const long long now_ms = rk_now_ms();
  if (now_ms - g_last_check_ms < 100)
    return;
  g_last_check_ms = now_ms;

  struct stat st;
  if (stat(path, &st) != 0) {
    return;
  }

  if (st.st_mtime == g_last_mtime_sec && st.st_mtim.tv_nsec == g_last_mtime_nsec)
    return;

  FILE *fp = fopen(path, "r");
  if (!fp) {
    LOG_W(NR_MAC, "RK-LA open failed path=%s\n", path);
    return;
  }

  rk_ue_la_clear_all();

  char line[512];
  int count = 0;

  while (fgets(line, sizeof(line), fp)) {
    char *p = line;
    while (isspace((unsigned char)*p)) p++;
    if (*p == '\0' || *p == '#')
      continue;

    int rnti = -1;
    int ul_max_mcs = RK_UE_LA_UNSET;
    int min_grant_prb = RK_UE_LA_UNSET;
    int ulsch_max_frame_inactivity = RK_UE_LA_UNSET;
    int pusch_target_snrx10 = RK_UE_LA_UNSET;
    float ul_sched_mul = -1.0f;
    int ul_maxcg_override = RK_UE_LA_UNSET;
    int ul_small_burst_bytes = RK_UE_LA_UNSET;
    float ul_small_burst_mul = -1.0f;

    char *saveptr = NULL;
    for (char *tok = strtok_r(p, " \t\r\n", &saveptr);
         tok != NULL;
         tok = strtok_r(NULL, " \t\r\n", &saveptr)) {
      if (parse_int_kv(tok, "rnti", &rnti)) continue;
      if (parse_int_kv(tok, "ul_max_mcs", &ul_max_mcs)) continue;
      if (parse_int_kv(tok, "min_grant_prb", &min_grant_prb)) continue;
      if (parse_int_kv(tok, "ulsch_max_frame_inactivity", &ulsch_max_frame_inactivity)) continue;
      if (parse_int_kv(tok, "pusch_target_snrx10", &pusch_target_snrx10)) continue;
      if (strncmp(tok, "ul_sched_mul=", 13) == 0) {
        ul_sched_mul = (float)atof(tok + 13);
        continue;
      }
      if (parse_int_kv(tok, "ul_maxcg_override", &ul_maxcg_override)) continue;
      if (parse_int_kv(tok, "ul_small_burst_bytes", &ul_small_burst_bytes)) continue;
      if (strncmp(tok, "ul_small_burst_mul=", 19) == 0) {
        ul_small_burst_mul = (float)atof(tok + 19);
        continue;
      }
    }

    if (rnti <= 0 || rnti > 0xFFFF) {
      LOG_W(NR_MAC, "RK-LA skip bad line: %s", line);
      continue;
    }

    rk_ue_la_entry_t *e = rk_ue_la_alloc((uint16_t)rnti);
    if (!e) {
      LOG_W(NR_MAC, "RK-LA table full, drop rnti=%d\n", rnti);
      continue;
    }

    e->ul_max_mcs = ul_max_mcs;
    e->min_grant_prb = min_grant_prb;
    e->ulsch_max_frame_inactivity = ulsch_max_frame_inactivity;
    e->pusch_target_snrx10 = pusch_target_snrx10;
    e->ul_sched_mul = ul_sched_mul;
    e->ul_maxcg_override = ul_maxcg_override;
    e->ul_small_burst_bytes = ul_small_burst_bytes;
    e->ul_small_burst_mul = ul_small_burst_mul;
    count++;
  }

  fclose(fp);

  g_last_mtime_sec = st.st_mtime;
  g_last_mtime_nsec = st.st_mtim.tv_nsec;

  LOG_I(NR_MAC, "RK-LA reload done path=%s count=%d\n", path, count);
}

int rk_ue_la_get_ul_max_mcs(uint16_t rnti, int default_v, int max_mcs_table)
{
  const rk_ue_la_entry_t *e = rk_ue_la_get_entry(rnti);
  int v = default_v;
  if (e && e->ul_max_mcs >= 0)
    v = e->ul_max_mcs;
  if (v > max_mcs_table)
    v = max_mcs_table;
  if (v < 0)
    v = 0;
  return v;
}

int rk_ue_la_get_min_grant_prb(uint16_t rnti, int default_v)
{
  const rk_ue_la_entry_t *e = rk_ue_la_get_entry(rnti);
  if (e && e->min_grant_prb > 0)
    return e->min_grant_prb;
  return default_v;
}

int rk_ue_la_get_ulsch_max_frame_inactivity(uint16_t rnti, int default_v)
{
  const rk_ue_la_entry_t *e = rk_ue_la_get_entry(rnti);
  if (e && e->ulsch_max_frame_inactivity >= 0)
    return e->ulsch_max_frame_inactivity;
  return default_v;
}

int rk_ue_la_get_pusch_target_snrx10(uint16_t rnti, int default_v)
{
  const rk_ue_la_entry_t *e = rk_ue_la_get_entry(rnti);
  if (e && e->pusch_target_snrx10 >= 0)
    return e->pusch_target_snrx10;
  return default_v;
}

float rk_ue_la_get_ul_sched_mul(uint16_t rnti, float default_v)
{
  const rk_ue_la_entry_t *e = rk_ue_la_get_entry(rnti);
  if (e && e->ul_sched_mul > 0.0f)
    return e->ul_sched_mul;
  return default_v;
}

int rk_ue_la_get_ul_maxcg_override(uint16_t rnti, int default_v)
{
  const rk_ue_la_entry_t *e = rk_ue_la_get_entry(rnti);
  if (e && e->ul_maxcg_override > 0)
    return e->ul_maxcg_override;
  return default_v;
}

int rk_ue_la_get_ul_small_burst_bytes(uint16_t rnti, int default_v)
{
  const rk_ue_la_entry_t *e = rk_ue_la_get_entry(rnti);
  if (e && e->ul_small_burst_bytes > 0)
    return e->ul_small_burst_bytes;
  return default_v;
}

float rk_ue_la_get_ul_small_burst_mul(uint16_t rnti, float default_v)
{
  const rk_ue_la_entry_t *e = rk_ue_la_get_entry(rnti);
  if (e && e->ul_small_burst_mul > 0.0f)
    return e->ul_small_burst_mul;
  return default_v;
}
