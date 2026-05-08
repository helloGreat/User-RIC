/*
 * Licensed to the OpenAirInterface (OAI) Software Alliance under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The OpenAirInterface Software Alliance licenses this file to You under
 * the OAI Public License, Version 1.1  (the "License"); you may not use this file
 * except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.openairinterface.org/?page_id=698
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *-------------------------------------------------------------------------------
 * For more information about the OpenAirInterface (OAI) Software Alliance:
 *      contact@openairinterface.org
 */

#include "ran_func_slice.h"
#include "../../flexric/test/rnd/fill_rnd_data_slice.h"
#include <assert.h>
#include <stdio.h>
#include <stdint.h>
/* from gNB_scheduler_dlsch.c */
extern void rk_slice_assoc_update_by_rnti(uint16_t rnti, int dl_id, int ul_id);

bool read_slice_sm(void* data)
{
  assert(data != NULL);
//  assert(data->type == SLICE_STATS_V0);

  slice_ind_data_t* slice = (slice_ind_data_t*)data;
  fill_slice_ind_data(slice);

  return true;
}

void read_slice_setup_sm(void* data)
{
  assert(data != NULL);
//  assert(data->type == SLICE_AGENT_IF_E2_SETUP_ANS_V0 );

  assert(0 !=0 && "Not supported");
}

sm_ag_if_ans_t write_ctrl_slice_sm(void const* data)
{
  assert(data != NULL);

  slice_ctrl_req_data_t const* slice_req_ctrl = (slice_ctrl_req_data_t const*)data;
  slice_ctrl_msg_t const* msg = &slice_req_ctrl->msg;

  if (msg->type == SLICE_CTRL_SM_V0_ADD) {
    printf("[E2 Agent]: SLICE CONTROL ADD rx\n");

  } else if (msg->type == SLICE_CTRL_SM_V0_DEL) {
    printf("[E2 Agent]: SLICE CONTROL DEL rx\n");

  } else if (msg->type == SLICE_CTRL_SM_V0_UE_SLICE_ASSOC) {
    printf("[E2 Agent]: SLICE CONTROL ASSOC rx\n");

    /* ===== RK SLICE PATCH V1: bridge UE assoc into scheduler state ===== */
    if (msg->u.ue_slice.ues != NULL && msg->u.ue_slice.len_ue_slice > 0) {
      for (size_t i = 0; i < msg->u.ue_slice.len_ue_slice; ++i) {
        ue_slice_assoc_t const* a = &msg->u.ue_slice.ues[i];

        printf("[RK-SLICE] UE_ASSOC bridge: rnti=%04x dl=%d ul=%d\n",
               (unsigned)a->rnti,
               (int)a->dl_id,
               (int)a->ul_id);

        rk_slice_assoc_update_by_rnti((uint16_t)a->rnti,
                                      (int)a->dl_id,
                                      (int)a->ul_id);
      }
    } else {
      printf("[RK-SLICE] UE_ASSOC rx but empty ue list\n");
    }
    /* ===== END RK SLICE PATCH V1 ===== */

  } else {
    assert(0 != 0 && "Unknown msg_type!");
  }

  sm_ag_if_ans_t ans = {.type = CTRL_OUTCOME_SM_AG_IF_ANS_V0};
  ans.ctrl_out.type = SLICE_AGENT_IF_CTRL_ANS_V0;
  return ans;
}


