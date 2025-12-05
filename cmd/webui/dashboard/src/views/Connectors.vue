<template>
  <a-flex style="  justify-content: space-between;">
    <h1>Connectors <span class="description">Manage your data source connection information(s).</span></h1>
    <div><a-button>+</a-button></div>
  </a-flex>
  <a-table :data-source="data" :columns="columns"></a-table>
</template>

<script setup lang="jsx">
const data = [
  {
    id: "0000-00000000-0000-000000000001",
    type: "Postgres",
    alias : "test_pg_1",
    host: "127.0.0.1",
    port : "5437",
    readers: 10,
    writers: 5,
    username : "anycdc_reader"
  },
  {
    id: "0000-00000000-0000-000000000002",
    type: "Postgres",
    alias : "test_pg_2",
    host: "127.0.0.1",
    port : "5437",
    readers: 10,
    writers: 5,
    username : "anycdc_reader"
  },
  {
    id: "0000-00000000-0000-000000000003",
    type: "MySQL",
    alias : "test_pg_1",
    host: "127.0.0.1",
    port : "3306",
    readers: 10,
    writers: 5,
    username : "anycdc_reader"
  },
]
const columns = []

for(var k in data[0]) {
  const item = {
    title : k,
    dataIndex:k,
    key:k,
  }
  if(k === "id") {
    item['customRender'] = (a)=> {
      const nav = '/connectors/' + a['value']
      console.log(nav)
      return (
          <a href={nav}>{a['value']}</a>
      )
    }
  }
  columns.push(item)

}
columns.push({
  title : "Action",
  key: "action",
  customRender : (_record)=>{
    return (
      <a-button disabled>Delete</a-button>
    )
  }
})




</script>