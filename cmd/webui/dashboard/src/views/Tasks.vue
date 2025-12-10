<template>
  <table-page
      title="Tasks"
      description="Manage your tasks information(s)."
      :custom-renders="customRenders"
      detail-prefix="/ui/common/tasks"
      object-name="tasks"
  ></table-page>
  <a-modal v-model:open="openLogs" style="width:80%">
    <log-group type="tasks" :object-id="currentTaskID" :key="currentTaskID"/>
  </a-modal>
</template>

<script setup lang="jsx">
import TablePage from "../components/TablePage.vue";
import LogGroup from "../components/LogGroup.vue";
import { ref } from 'vue';

import {APISDK} from "../services/api.js";
const sdk = new APISDK({})

let openLogs = ref(false);
let currentTaskID = ref("")

function handleLogs(record) {
  return async function()  {
    currentTaskID.value = record.record['id']
    openLogs.value = true
  }
}
function handleStart(record) {
  return function() {
    sdk.StartTask(record.record['id'])
  }
}

function handleStop(record) {
  return function() {
    sdk.StopTask(record.record['id'])
  }
}

function handleDelete(record) {
  return async function() {
    await sdk.Delete("tasks",record.record["id"])
    window.location.reload();
  }
}

const customRenders = {
  "__action__" : (record) => {
    return (
        <div class="action-groups">
          <a onClick={handleLogs(record)}>Logs</a>
          {record.record.status !== 'Running' && <a onClick={handleStart(record)}>Start</a>}
          {record.record.status === 'Running' && <a onClick={handleStop(record)}>Stop</a>}
          {record.record.status !== 'Running' && <a onClick={handleDelete(record)}>Delete</a>}
        </div>
    )
  },
  'info' : (record) => {
    return (<div>I <a>{record.record['metric_insert_count_since_started'] || 0}</a> /
      U <a >{record.record['metric_update_count_since_started'] || 0 }</a> /
      D <a>{record.record['metric_delete_count_since_started'] || 0 }</a>
    </div>)
  },
  'current_pos' : (record) => {
    return (<div>{record.record['last_position'] || 'null'}</div>)
  }
}

</script>