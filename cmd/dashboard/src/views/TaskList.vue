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
      <template #cdc_info="record,index,column">
        <a-tooltip :title="'last_cdc_at=' + record.record.text.last_cdc_at">
          <span v-if="record.record.text.last_cdc_at">{{formatGoTimeToNowDuration(record.record.text.last_cdc_at)}}</span>
          <span v-if="!record.record.text.last_cdc_at">--</span>
        </a-tooltip>
      </template>

      <template #tables="record,index,column">
        <div style="max-width:200px">{{record.record.text.tables}}</div>
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
  <a-modal v-model:open="openResyncUpdate" title="Change CDC Position To" @ok="onResyncUpdate">
    <a-textarea style="height: 5em" v-model:value="currentTable.last_dumper_key" placeholder="follow reader cdc position format,empty will be rotate to latest"></a-textarea>
    <p class="description">Please set to empty if want to full resync.</p>
  </a-modal>
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
const currentTable = ref({})
const openModal = ref(false)
const modalKey = ref((new Date()).toISOString())
const openPositionUpdate = ref(false)
const openResyncUpdate = ref(false)
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
  currentTable.value = record
  openResyncUpdate.value = true
  openModal.value = false
}

async function onResyncUpdate() {
  await sdk.TaskTableResync(currentTable.value['id'],currentTable.value['last_dumper_key'])
  window.location.reload()
}

function formatGoTimeToNowDuration(goTimeStr) {
  // 解析Golang time.Time字符串为JS Date对象
  const parseGoTime = (timeStr) => {
    if (!timeStr || typeof timeStr !== 'string') return null;

    // 适配Golang time.Time核心格式：提取 年月日 时分秒
    const timeRegex = /(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.\d+)?/;
    const match = timeStr.match(timeRegex);
    if (!match) return null;

    // 拼接为JS可解析的时间字符串
    const jsTimeStr = `${match[1]} ${match[2]}`;
    const date = new Date(jsTimeStr);
    return isNaN(date.getTime()) ? null : date;
  };

  // 1. 解析输入的Golang时间字符串
  const targetDate = parseGoTime(goTimeStr);
  if (!targetDate) {
    return 'Invalid time format';
  }

  // 2. 计算与当前时间（Date.now()）的间隔（取绝对值，单位：秒）
  const now = Date.now(); // 固定使用当前时间戳对比
  const diffSeconds = Math.abs(Math.floor((now - targetDate.getTime()) / 1000));

  // 3. 转换为时分秒的友好文本
  const hours = Math.floor(diffSeconds / 3600);
  const minutes = Math.floor((diffSeconds % 3600) / 60);
  const seconds = diffSeconds % 60;

  // 构建文本片段（仅保留非0的单位，秒数兜底）
  const parts = [];
  if (hours > 0) {
    parts.push(`${hours} ${hours === 1 ? 'hour' : 'hours'}`);
  }
  if (minutes > 0) {
    parts.push(`${minutes} ${minutes === 1 ? 'min' : 'mins'}`);
  }
  parts.push(`${seconds} ${seconds === 1 ? 'second' : 'seconds'}`);

  return parts.join(' ');
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