#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

typedef struct {
  int used;
  uint16_t rnti;
  int dl_max_mcs;
  int dl_min_grant_prb;
  float dl_sched_mul;
  int dl_maxcg_override;
  int dl_small_burst_bytes;
  float dl_small_burst_mul;
} rk_ue_dl_entry_t;

static rk_ue_dl_entry_t g_rk_ue_dl_tbl[128];
static time_t g_rk_ue_dl_mtime = 0;
static off_t g_rk_ue_dl_size = 0;
static int g_rk_ue_dl_loaded = 0;

static void rk_ue_dl_reset_tbl(void)
{
  for (int i = 0; i < (int)(sizeof(g_rk_ue_dl_tbl) / sizeof(g_rk_ue_dl_tbl[0])); ++i) {
    g_rk_ue_dl_tbl[i].used = 0;
    g_rk_ue_dl_tbl[i].rnti = 0;
    g_rk_ue_dl_tbl[i].dl_max_mcs = -1;
    g_rk_ue_dl_tbl[i].dl_min_grant_prb = -1;
    g_rk_ue_dl_tbl[i].dl_sched_mul = -1.0f;
    g_rk_ue_dl_tbl[i].dl_maxcg_override = -1;
    g_rk_ue_dl_tbl[i].dl_small_burst_bytes = -1;
    g_rk_ue_dl_tbl[i].dl_small_burst_mul = -1.0f;
  }
}

static rk_ue_dl_entry_t *rk_ue_dl_find_or_alloc(uint16_t rnti)
{
  rk_ue_dl_entry_t *free_slot = NULL;
  for (int i = 0; i < (int)(sizeof(g_rk_ue_dl_tbl) / sizeof(g_rk_ue_dl_tbl[0])); ++i) {
    if (g_rk_ue_dl_tbl[i].used && g_rk_ue_dl_tbl[i].rnti == rnti)
      return &g_rk_ue_dl_tbl[i];
    if (!g_rk_ue_dl_tbl[i].used && free_slot == NULL)
      free_slot = &g_rk_ue_dl_tbl[i];
  }
  if (free_slot != NULL) {
    free_slot->used = 1;
    free_slot->rnti = rnti;
    free_slot->dl_max_mcs = -1;
    free_slot->dl_min_grant_prb = -1;
    free_slot->dl_sched_mul = -1.0f;
    free_slot->dl_maxcg_override = -1;
    free_slot->dl_small_burst_bytes = -1;
    free_slot->dl_small_burst_mul = -1.0f;
  }
  return free_slot;
}

static const rk_ue_dl_entry_t *rk_ue_dl_find(uint16_t rnti)
{
  for (int i = 0; i < (int)(sizeof(g_rk_ue_dl_tbl) / sizeof(g_rk_ue_dl_tbl[0])); ++i) {
    if (g_rk_ue_dl_tbl[i].used && g_rk_ue_dl_tbl[i].rnti == rnti)
      return &g_rk_ue_dl_tbl[i];
  }
  return NULL;
}

static int rk_ue_dl_parse_line(char *line, rk_ue_dl_entry_t *e)
{
  char *saveptr = NULL;
  char *tok = strtok_r(line, " \t\r\n", &saveptr);
  int saw_rnti = 0;

  e->used = 0;
  e->rnti = 0;
  e->dl_max_mcs = -1;
  e->dl_min_grant_prb = -1;
  e->dl_sched_mul = -1.0f;
  e->dl_maxcg_override = -1;
  e->dl_small_burst_bytes = -1;
  e->dl_small_burst_mul = -1.0f;

  while (tok != NULL) {
    if (strncmp(tok, "rnti=", 5) == 0) {
      e->rnti = (uint16_t)strtoul(tok + 5, NULL, 10);
      e->used = 1;
      saw_rnti = 1;
    } else if (strncmp(tok, "dl_max_mcs=", 11) == 0) {
      e->dl_max_mcs = atoi(tok + 11);
    } else if (strncmp(tok, "dl_min_grant_prb=", 17) == 0) {
      e->dl_min_grant_prb = atoi(tok + 17);
    } else if (strncmp(tok, "dl_sched_mul=", 13) == 0) {
      e->dl_sched_mul = (float)atof(tok + 13);
    } else if (strncmp(tok, "dl_maxcg_override=", 18) == 0) {
      e->dl_maxcg_override = atoi(tok + 18);
    } else if (strncmp(tok, "dl_small_burst_bytes=", 21) == 0) {
      e->dl_small_burst_bytes = atoi(tok + 21);
    } else if (strncmp(tok, "dl_small_burst_mul=", 19) == 0) {
      e->dl_small_burst_mul = (float)atof(tok + 19);
    }
    tok = strtok_r(NULL, " \t\r\n", &saveptr);
  }

  return saw_rnti;
}

void rk_ue_dl_reload_if_needed(const char *path)
{
  struct stat st;
  if (path == NULL)
    return;

  if (stat(path, &st) != 0) {
    if (!g_rk_ue_dl_loaded)
      LOG_W(NR_MAC, "RK-LA DL open failed path=%s errno=%d\n", path, errno);
    return;
  }

  if (g_rk_ue_dl_loaded && st.st_mtime == g_rk_ue_dl_mtime && st.st_size == g_rk_ue_dl_size)
    return;

  FILE *fp = fopen(path, "r");
  if (!fp) {
    LOG_W(NR_MAC, "RK-LA DL open failed path=%s errno=%d\n", path, errno);
    return;
  }

  rk_ue_dl_reset_tbl();

  char buf[512];
  int count = 0;
  while (fgets(buf, sizeof(buf), fp) != NULL) {
    char line[512];
    snprintf(line, sizeof(line), "%s", buf);

    char *p = line;
    while (*p == ' ' || *p == '\t')
      ++p;
    if (*p == '#' || *p == '\n' || *p == '\0')
      continue;

    rk_ue_dl_entry_t tmp;
    if (!rk_ue_dl_parse_line(p, &tmp)) {
      LOG_W(NR_MAC, "RK-LA DL skip bad line: %s", buf);
      continue;
    }

    rk_ue_dl_entry_t *slot = rk_ue_dl_find_or_alloc(tmp.rnti);
    if (slot == NULL) {
      LOG_W(NR_MAC, "RK-LA DL table full, drop rnti=%d\n", (int)tmp.rnti);
      continue;
    }

    *slot = tmp;
    count++;
  }

  fclose(fp);

  g_rk_ue_dl_mtime = st.st_mtime;
  g_rk_ue_dl_size = st.st_size;
  g_rk_ue_dl_loaded = 1;

  LOG_I(NR_MAC, "RK-LA DL reload done path=%s count=%d\n", path, count);
}

int rk_ue_dl_get_max_mcs(uint16_t rnti, int default_val)
{
  const rk_ue_dl_entry_t *e = rk_ue_dl_find(rnti);
  if (e == NULL || e->dl_max_mcs < 0)
    return default_val;
  return e->dl_max_mcs;
}

int rk_ue_dl_get_min_grant_prb(uint16_t rnti, int default_val)
{
  const rk_ue_dl_entry_t *e = rk_ue_dl_find(rnti);
  if (e == NULL || e->dl_min_grant_prb < 0)
    return default_val;
  return e->dl_min_grant_prb;
}

float rk_ue_dl_get_sched_mul(uint16_t rnti, float default_val)
{
  const rk_ue_dl_entry_t *e = rk_ue_dl_find(rnti);
  if (e == NULL || e->dl_sched_mul <= 0.0f)
    return default_val;
  return e->dl_sched_mul;
}



int rk_ue_dl_get_maxcg_override(uint16_t rnti, int default_val)
{
  const rk_ue_dl_entry_t *e = rk_ue_dl_find(rnti);
  if (e == NULL || e->dl_maxcg_override <= 0)
    return default_val;
  return e->dl_maxcg_override;
}

int rk_ue_dl_get_small_burst_bytes(uint16_t rnti, int default_val)
{
  const rk_ue_dl_entry_t *e = rk_ue_dl_find(rnti);
  if (e == NULL || e->dl_small_burst_bytes <= 0)
    return default_val;
  return e->dl_small_burst_bytes;
}

float rk_ue_dl_get_small_burst_mul(uint16_t rnti, float default_val)
{
  const rk_ue_dl_entry_t *e = rk_ue_dl_find(rnti);
  if (e == NULL || e->dl_small_burst_mul <= 0.0f)
    return default_val;
  return e->dl_small_burst_mul;
}
