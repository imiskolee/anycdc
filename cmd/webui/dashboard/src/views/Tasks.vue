<template>
  <a-flex style="  justify-content: space-between;">
    <h1>Tasks <span class="description">Manage your tasks information(s).</span></h1>
    <div><a-button>+</a-button></div>
  </a-flex>
  <a-table :data-source="data" :columns="columns"></a-table>
</template>

<script setup lang="jsx">
const data = [
  {
    id: "0000-00000000-0000-000000000001",
    name: "test_pg_task_1",
    reader: "test_pg_1",
    writers : "test_pg_2 / test_mysql_1",
    status : "Active",
    delay : "~10s",
    processed : "100,021,023 rows / 4 tables",
    created_at : "2025-01-01 00:00:00",
  },
  {
    id: "0000-00000000-0000-000000000001",
    name: "test_pg_task_1",
    reader: "test_pg_1",
    writers : "test_pg_2 / test_mysql_1",
    status : "Active",
    delay : "~10s",
    processed : "100,021,023 rows / 4 tables",
    created_at : "2025-01-01 00:00:00",
  },
  {
    id: "0000-00000000-0000-000000000001",
    name: "test_pg_task_1",
    reader: "test_pg_1",
    writers : "test_pg_2 / test_mysql_1",
    status : "Active",
    delay : "~10s",
    processed : "100,021,023 rows / 4 tables",
    created_at : "2025-01-01 00:00:00",
  },
  {
    id: "0000-00000000-0000-000000000001",
    name: "test_pg_task_1",
    reader: "test_pg_1",
    writers : "test_pg_2 / test_mysql_1",
    status : "Active",
    delay : "~10s",
    processed : "100,021,023 rows / 4 tables",
    created_at : "2025-01-01 00:00:00",
  },
  {
    id: "0000-00000000-0000-000000000001",
    name: "test_pg_task_1",
    reader: "test_pg_1",
    writers : "test_pg_2 / test_mysql_1",
    status : "Active",
    delay : "~10s",
    processed : "100,021,023 rows / 4 tables",
    created_at : "2025-01-01 00:00:00",
  },
  {
    id: "0000-00000000-0000-000000000001",
    name: "test_pg_task_1",
    reader: "test_pg_1",
    writers : "test_pg_2 / test_mysql_1",
    status : "Active",
    delay : "~10s",
    processed : "100,021,023 rows / 4 tables",
    created_at : "2025-01-01 00:00:00",
  },
]
const columns = []

for(var k in data[0]) {
  columns.push({
    title : k,
    dataIndex:k,
    key:k,
  })
}
columns.push({
  title : "Action",
  key: "action",
  customRender : (_record)=>{
    return (
        <div>
          <a-button>Stop</a-button>
          <a-button disabled>Delete</a-button>
        </div>
    )
  }
})

</script>