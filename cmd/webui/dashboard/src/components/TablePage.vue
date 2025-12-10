<template>
  <a-flex style="  justify-content: space-between;">
    <h1>{{title}} <span class="description">{{description}}</span></h1>
    <div v-if="detailPrefix"><a :href="detailURL"><a-button>+</a-button></a></div>
  </a-flex>
  <a-table  :data-source="data" :columns="columns"></a-table>
</template>
<script setup lang="jsx">
import router from "../routers/index.js";
import {APISDK} from "../services/api.js";
import {onMounted, ref} from "vue";
import forms from "../services/forms.js"

const apiSDK = new APISDK({})

const props = defineProps({
  title : String,
  description: String,
  objectName: String,
  data: Array,
  detailPrefix: String,
  "custom-renders": Object
})

const objectName = props.objectName

const formDefination = forms[objectName]

const originCols = []

for(var k in formDefination) {
   const item = formDefination[k]
  const col = {
    title: item['name'],
    dataIndex: item['name'],
    key: item['name'],
  }
  originCols.push(col)
}



if(!props.detailPrefix) {
  props.detailPrefix= "/common/" + props.objectName
}

const detailPrefix = props.detailPrefix


let columns = ref(originCols)
let data = ref([])

const detailURL = props.detailPrefix + "/+"
if(!props.customRenders) {
  props.customRenders = {}
}

if(!props.customRenders["id"] && props.detailPrefix) {
  props.customRenders["id"] = (record)=>{
    const nav = props.detailPrefix + "/" + record['value']
    const id = record['text']
    return (
        <a href={nav}>{id}</a>
    )
  }
}

async function initData() {
  const r = await apiSDK.List(props.objectName)
  const resp = r[objectName]
  if(!resp) {
    return
  }
  const cols = []
  if(resp && resp.length > 0) {
    for(const k in formDefination) {
      if(formDefination[k]['hiddenOnList']) {
        continue
      }
      const col = formDefination[k]['name']
      const item = {
        title: col,
        dataIndex: col,
        key: col,
      }

      if(formDefination[k]['tips']) {
        item['customRender'] = (a)=> {
          return (
              <a-tooltip placement="top" trigger="hover" title={item.tips}>
                <span>{item.title}</span>
                <a-icon type="info"/>
              </a-tooltip>
          )
        }
      }
      if (props.customRenders && props.customRenders[col]) {
        item['customRender'] = props.customRenders[col]
      }
      if (!item['customRender'] && col=== "name") {
        item['customRender'] = (a) => {
          const nav = detailPrefix + '/' + a.record['id']
          return (
              <a href={nav}>{a['value']}</a>
          )
        }
      }
      cols.push(item)
    }

    if(props.customRenders && props.customRenders["__action__"]) {
      const item = {
        title: "Actions",
        key: "__Actions__",
        width: 100,
        customRender:props.customRenders["__action__"]
      }
      cols.push(item)
    }
    columns.value = cols
    data.value = resp
    console.log(columns,data)
}
}

onMounted(()=>{
  initData()
})


</script>