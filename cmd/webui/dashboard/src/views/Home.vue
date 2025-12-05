<template>
  <div>
  <a-row  :gutter="20">
    <a-col :span="12" >
      <a-card class="card" title="Connectors">
      <div class="card-href">
        <a href="/connectors" >6</a>
        <div class="small-text"><a href="#">3</a> Postgres / <a href="#">2</a> MySQL /  <a href="#">1</a> ES</div>
      </div>
      </a-card>
    </a-col>
    <a-col :span="12">
      <a-card class="card" title="Tasks">
      <div class="card-href">
      <a href="/tasks" >12</a>
        <div class="small-text"><a href="#">7</a> running / <a href="#">5</a> stooped</div>
      </div>
      </a-card>
    </a-col>
  </a-row>
  <div style="padding-top:40px;">
    <h3>System Information</h3>
    <a-table :datasource="data">

    </a-table>
  </div>
  </div>
</template>
<script setup>
const data = [
  {
    "field" : "Version",
    "value" : "0.0.1-dev"
  },
  {
    "field" : "Running Time",
    "value" : "10 days 24 hours"
  },
  {
    "field" : "Version",
    "value" : "0.0.1-dev"
  },
  {
    "field" : "Version",
    "value" : "0.0.1-dev"
  },
  {
    "field" : "Version",
    "value" : "0.0.1-dev"
  },
]
</script>

<style scoped>

.card-href {
  font-size:24px;
  text-align: center;
}
.small-text {
  font-size:14px;
  color:#666;
}
</style>
