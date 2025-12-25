<template>
<div>

  <any-form :model="connectorModel" :mode="mode" :value="detail" >
    <template #submit="record">
      <a-space size="small">
        <a-button danger :onclick="onTestConnection(record)">Test Connection</a-button>
        <a-button v-if="mode === 'create'" :onclick="onCreate(record)">Create</a-button>
        <a-button v-if="mode === 'edit'" :onclick="onEdit(record)">Save</a-button>
      </a-space>
    </template>
  </any-form>
</div>
</template>

<script setup>
import connectorModel from "../services/objects/connector.js"
import AnyForm from "../components/AnyForm.vue";
import {useRoute, useRouter} from "vue-router";
import sdk from "../services/api.js"
import {onMounted, reactive, ref} from "vue";
const route = useRoute()
const router = useRouter()
const detail = reactive({})
const id = route.params.id
const mode = id === '+' ? 'create' : 'edit'

function onTestConnection(record) {
  return async function() {
     await sdk.TestConnector(record.form)
  }
}

function onCreate(record) {
  return async function() {
    await sdk.Create("connectors",record.form)
    await router.push("/connectors")
  }
}

function onEdit(record) {
  return async function() {
    await sdk.Update("connectors",record.form['id'],record.form)
    await router.push("/connectors")
  }
}

async function init() {
  if(id !== '+') {
    const data = await sdk.Get("connectors",id)
    const resp = data['connectors']
    for(const d in resp) {
      detail[d] = resp[d]
    }
  }
}

onMounted(async ()=>{
  await init()
})

</script>