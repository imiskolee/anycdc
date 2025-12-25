<template>
  <a-select
            :mode="option.option_type"
            v-model:value.lazy="value">
    <a-select-option :value="option.value" v-for="option in option.options">{{option.name}}</a-select-option>
  </a-select>
</template>

<script setup>

const props = defineProps(
    {
      option : Object,
      value : String
    },
)

async function getDynamicOptions(table) {
  return (await sdk.)
}





</script>