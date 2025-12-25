<template>
  <div>
    <data-table :model="taskModel">
      <template #id="record,index,column">
        <router-link :to="'/tasks/' + record.record.text.id" >
          {{record.record.text.name}}
        </router-link>
      </template>
      <template #sync_info="record,index,column">
        {{record.record.text.last_cdc_position}}
      </template>
      <template #status="record,index,column">
        <a-tooltip trigger="hover" :title="'Status:' +record.record.text.status +  ' CDC Status:' + record.record.text.cdc_status">
          <div class="circle red" v-if="record.record.text.status === 'Active' && record.record.text.cdc_status === 'Failed'"></div>
          <div class="circle green" v-if="record.record.text.status === 'Active' && record.record.text.cdc_status === 'Running'"></div>
          <div class="circle black" v-if="record.record.text.status === 'Inactive'"></div>
          <div class="circle yellow" v-if="record.record.text.status === 'Active' && record.record.text.cdc_status !== 'Running' && record.record.text.cdc_status !== 'Failed'"></div>
        </a-tooltip>
      </template>

      <template #action="record,index,column">
        <a-button type="link" v-if="record.record.text.status !== 'Active'" :onclick="activeHandler(record)">Active</a-button>
        <a-button type="link" v-if="record.record.text.status === 'Active' && record.record.text.cdc_status !== 'Running'" danger :onclick="inactiveHandler(record)">InActive</a-button>
        <a-button type="link" v-if="record.record.text.cdc_status !== 'Running'" :onclick="startHandler(record)">Start</a-button>
        <a-button type="link" v-if="record.record.text.cdc_status === 'Running'" :onclick="stopHandler(record)">Stop</a-button>
        <a-button type="link" :onclick="openLogHandler(record)">Logs</a-button>
        <a-button type="link" :onclick="DeleteHandler(record)">Delete</a-button>

        <a-dropdown>
          <template #overlay>
            <a-menu >
              <a-menu-item v-for="item in actions" :key="item.name" :onclick="item.handler(record)">
                {{item.name}}
              </a-menu-item>
            </a-menu>
          </template>
          <a-button>
            More
          </a-button>
        </a-dropdown>
      </template>
    </data-table>
  </div>
  <a-modal v-model:open="openModal" :title="'Task:' + currentTask.name"  width="100%"
           wrap-class-name="full-modal" :key="modalKey">
    <a-tabs>
      <a-tab-pane key="1" tab="Details">
        <a-table :columns="tableLogColumns" :data-source="tableLogs">
          <template #bodyCell="{ column, record }">
            <div v-if="column.key === 'action'">
              <a-button type="link" @click="onResync(record)">Re-sync</a-button>
            </div>
            <div v-if="column.key !== 'action'">
{{record[column.key]}}
            </div>
          </template>
        </a-table>
      </a-tab-pane>
      <a-tab-pane key="2" tab="Logs">
        <log-tail :file-name="'tasks/' + currentTask.id + '.log'" :key="currentTask.id"/>
      </a-tab-pane>
    </a-tabs>
  </a-modal>
  <a-modal v-model:open="openPositionUpdate" title="Change CDC Position To" @ok="onPositionUpdate">
    <a-textarea style="height: 5em" v-model:value="currentTask.last_cdc_position" placeholder="follow reader cdc position format,empty will be rotate to latest"></a-textarea>
    <p class="description">Please set to empty if want to rotate to latest state.</p>
  </a-modal>
</template>
<script setup>
import DataTable from "../components/DataTable.vue";
import taskModel from "../services/objects/task.js"
import sdk from "../services/api.js"
import LogTail from "../components/LogTail.vue";
import {ref} from "vue";
const currentTask = ref({})
const openModal = ref(false)
const modalKey = ref((new Date()).toISOString())
const openPositionUpdate = ref(false)
function updateModalKey() {
  modalKey.value = (new Date()).toISOString()
}

const actions = [
  {
    name : "Enable debug logs",
    handler : function(record) {
      return async function() {
        await sdk.Update("tasks",record.record.value.id,{
          "debug_enabled" : true
        })
      }
    }
  },
  {
    name : "Disable debug logs",
    handler : function(record) {
      return async function() {
        await sdk.Update("tasks",record.record.value.id,{
          "debug_enabled" : false
        })
      }
    }
  },
  {
    name : "Rotate To",
    handler : function(record) {
      return function(){
        currentTask.value = record.record.value
        openPositionUpdate.value = true
      }
    }
  },
]

const tableLogColumns = [
  "table",
  "last_dumper_key",
  "total_dumped",
  "total_inserted",
  "total_updated",
  "total_deleted",
  "dumper_state",
  "action",
].map((v)=> {return {title:v,key:v,dataIndex:v}})

const tableLogs = ref([])

function activeHandler(record) {
  return async function() {
    await sdk.ActiveTask(record.record.value.id)
    window.location.reload()
  }
}

function inactiveHandler(record) {
  return async function() {
    await sdk.InactiveTask(record.record.value.id)
    window.location.reload()
  }
}


function startHandler(record) {
  return async function() {
    await sdk.StartTask(record.record.value.id)
    window.location.reload()
  }
}

function stopHandler(record) {
  return async function() {
    await sdk.StopTask(record.record.value.id)
    window.location.reload()
  }
}


function DeleteHandler(record) {
    return async function() {
      await sdk.Delete("tasks",record.record.value.id)
      window.location.reload()
    }
}


function openLogHandler(record) {
  return async function() {
    updateModalKey()
    currentTask.value = record.record.value
    openModal.value = true
    const resp = await sdk.GetTaskTableLogs(currentTask.value.id)
    tableLogs.value = resp['logs'] || []
  }
}

async function onPositionUpdate() {
  await sdk.TaskRotateTo(currentTask.value.id,{last_cdc_position:currentTask.value.last_cdc_position})
  openPositionUpdate.value = false;
  window.location.reload()
}

async function onResync(record) {
  await sdk.TaskTableResync(record['id'])
  window.location.reload()
}

</script>

<style scoped>
.circle {
  display: inline-block;
  width: 24px;
  height:24px;
  border-radius: 999px;
}
.circle.red {
  background-color: red;
}
.circle.green {
  background-color: green;
}
.circle.yellow {
  background-color: yellow;
}
.circle.black {
  background-color: black;
}
</style>