Task:
    Reader Reader
    Writers []Writer

    Prepare()
     self.Reader.Prepare()
     self.Writers.Prepare()

    Start()
        self.Reader.Start()
    
    Consume(event)
        self.Writers.Cosume(event)
    
    Stop()
        self.Save()
        self.Reader.Stop()
    
    Save()
        self.Reader.Save()

ReaderManager:
    Tasks []Task

    Save()
        self.Tasks.Save()


    
    
     