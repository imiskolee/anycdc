<template>
  <div>
    <div style="display: flex;justify-content: space-between">
      <h1>{{model.title}}  <span class="description">{{model.description}}</span></h1>
     <router-link :to="'/' + model.name + '/+'"><a-button>+</a-button></router-link>
    </div>
    <div>
      <a-table :columns="processedColumns" :data-source="dataSource">
        <template v-for="slotName in Object.keys($slots)" #[slotName]="slotProps">
          <slot :name="slotName" v-bind="slotProps"></slot>
        </template>
      </a-table>
    </div>
  </div>
</template>

<script setup>
import {computed, ref, useSlots} from "vue";
import sdk from "../services/api.js"

var props = defineProps(
    {
        model :Object,
    }
)

const columns = (props.model.columns || []).filter((v)=>!v.hiddenOnList);

columns.push({
  name : "action"
})

const dataSource = ref([])

const slots = useSlots();

const processedColumns = computed(() => {
  return columns.map((column) => {
    // 列的字段名（dataIndex 或 key）
    const fieldName = column.name
    column.key = fieldName
    column.title = fieldName
    if (!fieldName) return column;
    return {
      ...column,
      customRender: (record,index,column) => {
        const slot = slots[fieldName];
        if (slot) {
          return slot({record, index,column});
        }
        const val = record.text[fieldName]
        if(!val) {
          return "null"
        }
        return val
      },
    };
  });
});

async function initDataSource() {
  const resp = await sdk.List(props.model.name)
  const list = resp[props.model.name]
  if(!list) {
    return;
  }
  dataSource.value = list;
}

initDataSource()

</script>

<style scoped>
.description {
  font-size:12px;
  color:#999;
}
</style>

