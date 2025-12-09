<template>
<div>
  <div>
    <h1 v-if="mode === 'create'">Create {{objectName}}</h1>
    <h1 v-if="mode === 'edit'">Edit {{objectName}}</h1>
  </div>
  <div>
    <a-form
        layout="horizontal"
        :label-col="labelCol"
        :wrapper-col="wrapperCol"
        id="form"
    >
      <a-form-item v-for="item in metadata" :label="item.name" :name="item.name" :key="item.name">
        <a-input v-if="item.type === 'string'" v-model:value.lazy="obj[item.name]" :disabled="item.readonly" :placeholder="item.placeholder"/>
        <a-input v-if="item.type === 'number'" type="number" v-model:value.lazy="obj[item.name]"  :placeholder="item.placeholder" />
        <a-textarea v-if="item.type === 'json'" style="min-height: 5em" v-model:value.lazy="obj[item.name]"  :placeholder="item.placeholder" ></a-textarea>

        <a-select v-if="item.type === 'options' || item.type === 'dynamic_options'"
                  :mode="item.option_type"
                  v-model:value.lazy="obj[item.name]">
          <a-select-option :value="option.value" v-for="option in item.options">{{option.name}}</a-select-option>
        </a-select>

      </a-form-item>
      <div style="align-content: center;text-align: center">
        <a-button :onclick="handleSubmit">Submit</a-button>
      </div>
    </a-form>
  </div>
</div>
</template>

<script setup>
import {useRoute, useRouter} from 'vue-router'
import forms from "../services/forms.js"
import {APISDK} from "../services/api.js";
import {onMounted, reactive, ref} from 'vue'
import {notification} from "ant-design-vue";
const labelCol = {span:8}
const wrapperCol = { span: 16 }
const route = useRoute()
const router = useRouter()
const id = route.params.id
const mode = id === "+" ? "create" : "edit"
const objectName = route.params.object
const metadata = reactive(forms[objectName])

const obj = reactive({})

const apiSDK = new APISDK(
    {

    }
)

async function handleSubmit() {
  try {
    const data = toData(obj)

    if (mode === 'create') {
      await apiSDK.Create(objectName, data)
    } else if (mode === 'edit') {
      await apiSDK.Update(objectName, id, data)
    }
    router.push("/" + objectName)
  }catch(e) {
    notification.error({
      message : "Failed",
      description : e,
    })
  }
}

async function init() {
  if (mode === 'edit') {
    try {
      const resp = await apiSDK.Get(objectName, id)
      const data = parseData(resp[objectName])
      for(const k in data) {
        obj[k] = data[k]
      }
    }catch(e) {

    }
  }
}

onMounted(async ()=>{
  for(const key in metadata) {
    const item = metadata[key]
    if(item.type === 'dynamic_options') {
      const lst = await getDynamicOptions(item['data_source'])
      const items = []
      lst.forEach((l)=>{
        items.push({
          name : l.name,
          value : l.id
        })
      })
      metadata[key]['options'] = items
    }
  }
  await init()
})

async function getDynamicOptions(table) {
  return (await apiSDK.List(table))[table]
}


function parseData(record) {
  const newData = {}
  metadata.forEach((f)=> {
    if(((f.option_type && f.option_type === 'multiple' )) && record[f.name]) {
      newData[f.name] = JSON.parse(record[f.name])
    }else{
      newData[f.name] = record[f.name]
    }
  })
  return newData
}

function toData(record) {
  const newData = {}
  metadata.forEach((f)=> {
    if(((f.option_type && f.option_type === 'multiple' && record[f.name])) && record[f.name]) {
      newData[f.name] = JSON.stringify(record[f.name])
    }else{
      newData[f.name] = record[f.name]
    }
  })
  return newData
}




</script>