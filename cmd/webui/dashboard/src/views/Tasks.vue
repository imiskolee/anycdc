<template>
  <table-page
      title="Tasks"
      description="Manage your tasks information(s)."
      :custom-renders="customRenders"
      detail-prefix="/ui/common/tasks"
      object-name="tasks"
      :key="key"
  ></table-page>
  <a-modal  v-model:open="openLogs" style="width:80%">
    <log-group type="tasks" :object-id="currentTaskID" :key="currentTaskID"/>
  </a-modal>
</template>

<script setup lang="jsx">
import TablePage from "../components/TablePage.vue";
import LogGroup from "../components/LogGroup.vue";
import ActionMenu from "../components/ActionMenu.vue";

import {h, ref} from 'vue';

import {APISDK} from "../services/api.js";
const sdk = new APISDK({})

let openLogs = ref(false);
let currentTaskID = ref("")
let key = ref((new Date()).toISOString())

function refresh() {
  key.value = (new Date()).toISOString()
}

function handleLogs(record) {
  return async function () {
    currentTaskID.value = record.record['id']
    openLogs.value = true
    refresh();
  }
}

  function handleActive(record) {
    return async function () {
      await sdk.ActiveTask(record.record['id'])
      refresh();
    }
  }

  function handleInactive(record) {
    return async function () {
      await sdk.InactiveTask(record.record['id'])
      refresh();
    }
  }

  function handleStart(record) {
    return async function () {
      await sdk.StartTask(record.record['id'])
      refresh();
    }
  }

  function handleStop(record) {
    return async function () {
      await sdk.StopTask(record.record['id'])
      refresh();
    }
  }

  function handleDelete(record) {
    return async function () {
      await sdk.Delete("tasks", record.record["id"])
      refresh();
    }
  }


/**
 *
 * @type {{current_pos: (function(*): *), __action__: (function(*): *), info: (function(*): *), status: (function(*): *)}}

 <div class="action-groups" style="width:200px">
 <a onClick={handleLogs(record)}>Logs</a>
 {record.record.status !== 'Active' && <a onClick={handleActive(record)}>Active</a>}
 {record.record.status === 'Active' && record.record.runner_status !== 'Running' && <a onClick={handleInactive(record)}>Inactive</a>}
 {record.record.runner_status === 'Running' && <a onClick={handleStop(record)}>Stop</a>}
 {record.record.runner_status !== 'Running' && <a onClick={handleStart(record)}>Start</a>}
 {record.record.status !== 'Active' && <a onClick={handleDelete(record)}>Delete</a>}
 </div>

 */



const customRenders = {
    "__action__": (record) => {

      const actionMenus = [
        {
          name : "Active",
        },
        {
          name : "Inactive"
        },
        {
          name : "Start",
        },
        {
          name : "Stop",
        },
        {
          name : "Logs",
        },
        {
          name : "Rotate To Latest",
        },
        {
          name : "Open Debug"
        },
        {
          name : "Close Debug"
        },
        {
          name : "Delete",
        }
      ]
    },
    'info': (record) => {
      return (<div>I <a>{record.record['metric_insert_count_since_started'] || 0}</a> /
        U <a>{record.record['metric_update_count_since_started'] || 0}</a> /
        D <a>{record.record['metric_delete_count_since_started'] || 0}</a>
      </div>)
    },
    'status': (record) => {
      let cls = "yellow"
      let status = record.record['status']
      let runner_status = record.record['runner_status']
      if (status === 'Inactive') {
        cls = "black"
      } else if (status === 'Active' && runner_status === 'Running') {
        cls = "green"
      } else if (status === 'Active' && runner_status === 'Failed') {
        cls = "red"
      }
      cls = "circle " + cls
      const title = `Status: ${status}, Runner: ${runner_status}`
      return (
          <div style="width:30px;">
            <a-tooltip trigger="hover" title={title}>
              <div class={cls}></div>
            </a-tooltip>
          </div>
      )
    },
    'current_pos': (record) => {
      return (<div>{record.record['last_position'] || 'null'}</div>)
    }
  }


</script>

<style>
.circle {
  width:20px;
  height:20px;
  border-radius: 999px;
}
.circle.black {
  background: #181818;
}
.circle.red {
  background: #af1c1c;
}
.circle.green {
  background: #28af28;
}
.circle.yellow {
  background: #afaf1d;
}
</style>