<template>
  <div>
    <data-table :model="connectorModel">
      <template #id="record,index,column">
        <router-link :to="'/connectors/' + record.record.text.id" >
          {{record.record.text.name}}
        </router-link>
      </template>
      <template #action="record,index,column">
        <a-button type="link" :onclick="TestConnectionHandler(record)">Test Connection</a-button>
        <a-button type="link" :onclick="DeleteHandler(record)">Delete</a-button>
      </template>
    </data-table>
  </div>
</template>
<script setup>
import DataTable from "../components/DataTable.vue";
import connectorModel from "../services/objects/connector.js"
import sdk from "../services/api.js"

function TestConnectionHandler(record) {
  return function() {
    sdk.TestConnector(record.record.value)
  }
}

function DeleteHandler(record) {
  return async function() {
    await sdk.Delete("connectors",record.record.value.id)
    window.location.reload()
  }
}

</script>