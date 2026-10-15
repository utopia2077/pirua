console.log("hello", 1, {a:true});
text({count:2});
image(await tools["mcp/read-image"]({path:"example.png"}));
return ALL_TOOLS.map(x=>x.name);
