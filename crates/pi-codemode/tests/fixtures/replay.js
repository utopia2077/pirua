const before = [Date.now(), performance.now(), Math.random(), new Date().toISOString(), new Date(0).getHours(), new Date(0).getTimezoneOffset()];
const order=[];
await Promise.all([tools.wait("a").then(x=>order.push(x)),tools.wait("b").then(x=>order.push(x))]);
return [before,order,[Date.now(),performance.now()]];
