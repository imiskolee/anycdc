package schemas

type Table struct {
	Name    string
	Columns []Column
}

func (t *Table) Exists(name string) bool {
	for _, c := range t.Columns {
		if c.Name == name {
			return true
		}
	}
	return false
}

func (t *Table) GetPrimaryKeys() []Column {
	var pks []Column
	for _, c := range t.Columns {
		if c.IsPrimaryKey {
			pks = append(pks, c)
		}
	}
	return pks
}

func (t *Table) GetPrimaryKeyNames() []string {
	var names []string
	pks := t.GetPrimaryKeys()
	for _, pk := range pks {
		names = append(names, pk.Name)
	}
	return names
}

func (s *Table) GetFieldByIndex(idx uint) (Column, bool) {
	for _, f := range s.Columns {
		if idx == f.Index {
			return f, true
		}
	}
	return Column{}, false
}

func (s *Table) GetFieldByName(name string) (Column, bool) {
	for _, f := range s.Columns {
		if name == f.Name {
			return f, true
		}
	}
	return Column{}, false
}
