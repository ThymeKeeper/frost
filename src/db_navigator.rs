// src/db_navigator.rs
use std::path::PathBuf;
use std::fs::{self, OpenOptions};
use std::io::Write;
use anyhow::Result;
use crate::schema_cache::{SchemaCache, ObjectType};

pub struct DbNavigator {
    data_dir: PathBuf,
    cache: Option<SchemaCache>,
}

impl DbNavigator {
    pub fn new() -> Self {
        // Use the same approach as config - everything in the executable directory
        let data_dir = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|p| p.to_path_buf()))
            .and_then(|p| std::fs::canonicalize(p).ok())
            .unwrap_or_else(|| PathBuf::from("."));
        
        //eprintln!("DbNavigator using data_dir: {}", data_dir.display());
        
        Self {
            data_dir,
            cache: None,
        }
    }
    
    pub fn request_refresh(&self, command: &str) -> Result<()> {
        let queue_path = self.data_dir.join("crawler_queue.txt");
        
        // Retry a few times if locked
        for attempt in 0..5 {
            match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&queue_path)
            {
                Ok(mut file) => {
                    #[cfg(unix)]
                    {
                        use fs2::FileExt;
                        if let Err(_) = file.try_lock_exclusive() {
                            std::thread::sleep(std::time::Duration::from_millis(100));
                            continue;
                        }
                    }
                    
                    writeln!(file, "{}", command)?;
                    
                    #[cfg(unix)]
                    {
                        use fs2::FileExt;
                        file.unlock()?;
                    }
                    
                    self.launch_crawler();
                    return Ok(());
                }
                Err(_) if attempt < 4 => {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        
        Err(anyhow::anyhow!("Could not write to queue"))
    }

    fn launch_crawler(&self) {
        use std::process::Command;
        
        //eprintln!("DbNavigator data_dir: {}", self.data_dir.display());
        
        let exe_path = std::env::current_exe().unwrap_or_default();
        let crawler_name = if cfg!(windows) {
            "Frost-crawler.exe"
        } else {
            "Frost-crawler"
        };
        
        let crawler_exe = exe_path
            .parent()
            .map(|p| p.join(crawler_name))
            .unwrap_or_else(|| PathBuf::from(crawler_name));

        //eprintln!("Launching crawler: {}", crawler_exe.display());
        //eprintln!("With --data-dir: {}", self.data_dir.display());
        
        let _ = Command::new(&crawler_exe)
            .arg("--data-dir")
            .arg(&self.data_dir)
            .spawn();

        //eprintln!("Crawler launch attempted");
    }
    
    pub fn load_cache(&mut self) -> Result<SchemaCache> {
        if self.cache.is_none() {
            let cache_path = self.data_dir.join("schema_cache.json");
            
            if !cache_path.exists() {
                // Request initial load
                self.request_refresh("REFRESH ALL")?;
                
                // Wait for cache to be created
                for _ in 0..20 {
                    if cache_path.exists() {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }
            }
            
            let content = fs::read_to_string(&cache_path)?;
            self.cache = Some(serde_json::from_str(&content)?);
        }
        
        Ok(self.cache.as_ref().unwrap().clone())
    }

    pub fn clear_cache(&mut self) {
        self.cache = None;
    }
    
    pub fn get_databases(&mut self) -> Result<Vec<String>> {
        let cache = self.load_cache()?;
        Ok(cache.databases.keys().cloned().collect())
    }
    
    pub fn get_schemas(&mut self, database: &str) -> Result<Vec<String>> {
        let cache = self.load_cache()?;
        
        if let Some(db) = cache.databases.get(database) {
            Ok(db.schemas.keys().cloned().collect())
        } else {
            Ok(Vec::new())
        }
    }
    
    pub fn get_tables(&mut self, database: &str, schema: &str) -> Result<Vec<(String, ObjectType)>> {
        let cache = self.load_cache()?;
        
        if let Some(db) = cache.databases.get(database) {
            if let Some(sch) = db.schemas.get(schema) {
                return Ok(sch.objects.iter()
                    .filter(|(_, obj)| matches!(obj.object_type, ObjectType::Table | ObjectType::View))
                    .map(|(name, obj)| (name.clone(), obj.object_type.clone()))
                    .collect());
            }
        }
        
        Ok(Vec::new())
    }
    
    pub fn get_columns(&mut self, database: &str, schema: &str, table: &str) -> Result<Vec<(String, String)>> {
        let cache = self.load_cache()?;
        
        if let Some(db) = cache.databases.get(database) {
            if let Some(sch) = db.schemas.get(schema) {
                if let Some(tbl) = sch.objects.get(table) {
                    return Ok(tbl.columns.iter()
                        .map(|col| (col.name.clone(), col.data_type.clone()))
                        .collect());
                }
            }
        }
        
        Ok(Vec::new())
    }
}