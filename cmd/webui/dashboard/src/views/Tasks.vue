<template>
  <table-page
      title="Tasks"
      description="Manage your tasks information(s)."
      :custom-renders="customRenders"
      detail-prefix="/common/tasks"
      object-name="tasks"
  ></table-page>
  <a-modal v-model:open="openLogs" style="width:80%">
    <log-group />
  </a-modal>
</template>

<script setup lang="jsx">
import TablePage from "../components/TablePage.vue";
import LogGroup from "../components/LogGroup.vue";
import { ref } from 'vue';
let openLogs = ref(false);

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

function handleLogs() {
  openLogs.value = true
  console.log("Open Logs...")
}

const customRenders = {
  "__action__" : (record) => {
    return (
        <div class="action-groups">
          <a onClick={handleLogs}>Logs</a>
          <a href="#">Stop</a>
          <a href="#">Delete</a>
        </div>
    )
  }
}

</script>