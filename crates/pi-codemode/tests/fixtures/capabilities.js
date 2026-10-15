let imported=false;
try{await import("node:fs")}catch{imported=true}
return [typeof process,typeof require,typeof fetch,typeof readFile,typeof setTimeout,typeof WeakRef,typeof FinalizationRegistry,Object.isFrozen(Array.prototype),Object.isFrozen(tools),imported];
