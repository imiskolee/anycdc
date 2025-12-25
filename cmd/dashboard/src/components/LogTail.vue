<template>
  <div>
    <div class="log-title"><h2>{{fileName}}</h2> </div>
    <div class="log-panel">
      <pre>
        {{content}}
      </pre>
    </div>
    <div>
      <a-space size="small">
        <a-button :onclick="refresh">Refresh</a-button>
        <a-button :onclick="readContent" :disabled="isEnd" primary>More</a-button>
      </a-space>

    </div>
  </div>
</template>

<script setup>
import sdk from "../services/api.js"
import {onMounted, ref} from "vue";

let currentPos = 0;
let content = ref("")
let isEnd = ref(false)
const lines = 50;
var props = defineProps(
    {
      fileName: String
    }
)
async function readContent() {
  const resp = await sdk.LogTail({
    file : props.fileName,
    pos : currentPos,
    lines:lines,
  })
  const data = resp['logs'] || {}
  content.value = content.value + data['content'] || ''
  if(data['next_pos'] >0 ) {
    currentPos = data['next_pos']
  }else {
    isEnd.value = true
  }
}

async function refresh() {
  currentPos = 0;
  content.value = ''
  isEnd.value = false;
  await readContent()
}

onMounted(()=> {
  readContent()
})





</script>

<style scoped>
.log-panel pre {
  color:#fff;
  background-color: #181818;
  font-size:12px;
  line-height: 16px;
  font-weight: 200;
  height:80%;
  overflow: scroll;
  min-height: 40em;
  max-height: 40em;

}
</style>