package model

func GetConnectorByName(name string) (*Connector, error) {
	var connector Connector
	if err := DB().Model(connector).Where("name = ?", name).Last(&connector).Error; err != nil {
		return nil, err
	}
	return &connector, nil
}

func GetConnectorByID(id string) (*Connector, error) {
	var connector Connector
	if err := DB().Model(connector).Where("id = ?", id).Last(&connector).Error; err != nil {
		return nil, err
	}
	return &connector, nil
}

func GetTaskByID(id string) (*Task, error) {
	var task Task
	if err := DB().Model(task).Where("id = ?", id).Last(&task).Error; err != nil {
		return nil, err
	}
	return &task, nil
}

func GetTaskByName(name string) (*Task, error) {
	var task Task
	if err := DB().Model(task).Where("name = ?", name).Last(&task).Error; err != nil {
		return nil, err
	}
	return &task, nil
}

func GetTasks() ([]Task, error) {
	var tasks []Task
	if err := DB().Model(tasks).Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}
