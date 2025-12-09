<template>
  <a-flex style="justify-content: space-between;">
    <h1>Add Tasks</h1>
  </a-flex>
  <div class="form">
    <a-form layout="horizontal"
            :label-col="labelCol"
            :wrapper-col="wrapperCol">
      <a-form-item label="Name" name="Name">
        <a-input />
      </a-form-item>
      <a-form-item label="Reader" name="Reader">
       <a-select>
         <a-select-option value="1">test_pg_1</a-select-option>
         <a-select-option value="2">test_pg_2</a-select-option>
         <a-select-option value="3">test_pg_3</a-select-option>
         <a-select-option value="4">test_pg_4</a-select-option>
       </a-select>
      </a-form-item>
      <a-form-item label="Writers" name="Writers">
        <div>
          <a-select
              mode="multiple"
          >
            <a-select-option value="1">test_pg_1</a-select-option>
            <a-select-option value="2">test_pg_2</a-select-option>
            <a-select-option value="3">test_pg_3</a-select-option>
            <a-select-option value="4">test_pg_4</a-select-option>
          </a-select>
        </div>
      </a-form-item>
      <a-form-item label="Tables" name="Tables">
        <a-textarea style="min-height: 8em" placeholder="table_1 | table_2 | table_3 | N"/>
      </a-form-item>
      <a-form-item label="Extras" name="Extras">
        <a-textarea style="min-height: 8em"/>
      </a-form-item>
      <a-form-item>
      </a-form-item>
      <div style="align-content: center;text-align: center">
        <a-button>提交</a-button>
      </div>
    </a-form>
  </div>
</template>
<script setup lang="ts">
const labelCol = {span:8}
const wrapperCol = { span: 16}
</script>

<style scoped>

</style>