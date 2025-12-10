<template>
  <div class="log-panel">
  <pre>
    {{content}}
  </pre>
  </div>
</template>

<style scoped>
.log-panel {
  padding:4px;
  background: #181818;
  color:#fff;
  font-size:12px;
  font-weight: 100;
  line-height: 14px;
  min-height:600px;
  max-height: 600px;
  overflow: scroll;
}
</style>
<script setup lang="ts">
import {onMounted, ref} from "vue";

const props = defineProps({
  type: String,
  id: String
})

import {APISDK} from "../services/api.js";
const sdk = new APISDK({})
const content = ref("")
onMounted(async ()=>{
  const log = await sdk.GetTaskLog(props.id)
  content.value = log['log'] || ''
})


</script>