const a=load("old");a.nested.push(2);
text(load("old").nested.length);
store("new",a);a.nested.push(3);
store("old",undefined);
exit();
text("unreachable");
