<template>
  <div>
    <any-form :model="taskModel" :mode="mode" :value="detail" >
      <template #submit="record">
        <a-space size="small">
          <a-button v-if="mode === 'create'" :onclick="onCreate(record)">Create</a-button>
          <a-button v-if="mode === 'edit'" :onclick="onEdit(record)">Save</a-button>
        </a-space>
      </template>
    </any-form>
  </div>
</template>
<script setup>
import taskModel from "../services/objects/task.js"
import AnyForm from "../components/AnyForm.vue";
import {useRoute, useRouter} from "vue-router";
import sdk from "../services/api.js"
import {onMounted, reactive, ref} from "vue";
const route = useRoute()
const router = useRouter()
const detail = reactive({})
const id = route.params.id
const mode = id === '+' ? 'create' : 'edit'


function toData(form) {
  if(typeof form['writers'] === 'string') {
    form['writers'] = [form['writers']]
  }
  if(typeof form['writers'] === 'object') {
    form['writers'] = JSON.stringify(form['writers'])
  }
  return form
}

function onCreate(record) {
  return async function() {
    await sdk.Create("tasks",toData(record.form))
    await router.push("/tasks")
  }
}

function onEdit(record) {
  return async function() {
    await sdk.Update("tasks",record.form['id'],toData(record.form))
    await router.push("/tasks")
  }
}

async function init() {
  if(id !== '+') {
    const data = await sdk.Get("tasks",id)
    const resp = data['tasks']
    for(const d in resp) {
      detail[d] = resp[d]
      if(detail['writers']) {
        detail['writers'] = JSON.parse(detail['writers']);
      }
    }
  }
}

onMounted(async ()=>{
  await init()
})

</script>