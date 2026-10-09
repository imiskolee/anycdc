
<template>
  <div>
    <h1 v-if="mode === 'create'">Create {{model.title}}</h1>
    <h1 v-if="mode === 'edit'"> Edit {{model.title}} - {{value.id}}</h1>
  </div>
  <a-form
      layout="horizontal"
      :label-col="labelCol"
      :wrapper-col="wrapperCol"
      id="form"
  >
    <a-form-item v-for="item in visible" :label="item.name" :name="item.name" :key="item.name">
      <a-input v-if="item.type === 'string'" v-model:value="obj[item.name]" :disabled="item.readonly" :placeholder="ph(item)"/>
      <a-input v-if="item.type === 'number'" type="number" v-model:value.number="obj[item.name]"  :placeholder="ph(item)" />
      <a-textarea v-if="item.type === 'json'" style="min-height: 5em" v-model:value="obj[item.name]"  :placeholder="ph(item)" ></a-textarea>

      <a-select v-if="item.type === 'options' || item.type === 'dynamic_options'"
                :mode="item.option_type"
                v-model:value="value[item.name]">
        <a-select-option :value="option.value" v-for="option in item.options">{{option.name}}</a-select-option>
      </a-select>
      <a-switch v-if="item.type === 'switch'" v-model:checked="value[item.name]" />
      <div v-if="item.tip && (!item.tipFor || item.tipFor.includes(obj.type))" class="field-tip" style="white-space: pre-wrap; font-size:12px; color:#888; margin-top:4px; line-height:1.5;">{{ item.tip }}</div>
    </a-form-item>
    <a-form-item>
      <div style="display: flex;justify-content: center">
        <slot name="submit" :form ="obj"></slot>
      </div>
    </a-form-item>
    <div>
    </div>
  </a-form>
</template>
<script setup>

import sdk from "../services/api.js"
import {onMounted, reactive, ref, computed} from "vue";

const props = defineProps(
    {
      model : Object,
      value : Object,
      mode : String
    }
)
const labelCol = {span:8}
const wrapperCol = { span: 16 }
const metadata = ref((props.model.columns || []).filter((item)=>item.type))

const obj = reactive(props.value)

// 按当前 type 过滤掉 hideFor 中列出的、与本类型无关的字段；
// visibleFor 则限定仅在某些类型下显示。
const visible = computed(() =>
    metadata.value.filter((item) => {
      if (item.visibleFor && !item.visibleFor.includes(obj.type)) return false
      if (item.hideFor && item.hideFor.includes(obj.type)) return false
      return true
    })
)

// 取占位符：支持按当前 type 指定(placeholderFor)，否则用通用 placeholder。
function ph(item) {
  if (item.placeholderFor && item.placeholderFor[obj.type]) return item.placeholderFor[obj.type]
  return item.placeholder
}

onMounted(async ()=>{
  for(const key in metadata.value) {
    const item = metadata.value[key]
    if(item.type === 'dynamic_options') {
      const lst = await getDynamicOptions(item['data_source'])
      const items = []
      lst.forEach((l)=>{
        items.push({
          name : l.name,
          value : l.id
        })
      })
      metadata.value[key]['options'] = items
    }
  }
})





async function getDynamicOptions(table) {
  return (await sdk.List(table))[table]
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
    }else if(f.type === 'number') {
      newData[f.name] = Number(record[f.name])
    } else{
      newData[f.name] = record[f.name]
    }
  })
  return newData
}

</script>
