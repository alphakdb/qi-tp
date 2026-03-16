/ Based on Kx's https://github.com/KxSystems/kdb-tick

.qi.import`mon

.tp.regfeed:{[pkg]
  if[`schemas in key .tp;if[pkg in .tp.schemas;:()]];
  .tp.schemas,:pkg;
  t1:key .schemas.t;
  .qi.importx[`schemas;pkg];
  if[not count t2:key[.schemas.t]except t1;:()];
  .qi.info"Dynamically adding ",","sv string t2;
  .u.t:.u.t union t2;
  .u.w,:t2!count[t2]#enlist (.u.suballs,'`);
  }

\d .u

w:(0#`)!()
.qi.frompkg[`tp;`u];  

ld:{if[not type key L::`$(-10_string L),string x;.[L;();:;()]];i::j::-11!(-2;L);if[0<=type i;-2 (string L)," is a corrupt log. Truncate to length ",(string last i)," and restart";exit 1];hopen L};
tick:{init[];if[not min(`time`sym~2#key flip value@)each t;'`timesym];@[;`sym;`g#]each t;d::.z.D;;L::.qi.path(.conf.DATA;.proc.self.stackname;`tplogs;.proc.self.name;"tp",10#".");l::ld d};

endofday:{end d;d+:1;if[l;hclose l;l::0(`.u.ld;d)]};
ts:{if[d<x;if[d<x-1;system"t 0";'"more than one day?"];endofday[]]};

if[.conf.TP_BATCH_PERIOD;
  .event.addhandler[`.z.ts;{.u.pub'[.u.t;value each .u.t];@[`.;.u.t;@[;`sym;`g#]0#];i::.u.j;.u.ts .z.D}];
  upd:{[t;x]
  if[not -12=type first first x;if[d<"d"$a:.z.P;.z.ts[]];a:"n"$a;x:$[0>type first x;a,x;(enlist(count first x)#a),x]];
  t insert x;if[l;l enlist (`upd;t;x);j+:1];}];

if[not .conf.TP_BATCH_PERIOD;
 .event.addhandler[`.z.ts;{.u.ts .z.D}];
 upd:{[t;x]ts"d"$a:.z.P;x:@[x;-1+count x;:;.z.p];
 if[not -12=type first first x;a:"p"$a;x:$[0>type first x;a,x;(enlist(count first x)#a),x]];
 f:cols .schemas.t t;pub[t;$[0>type first x;enlist f!x;flip f!x]];if[l;l enlist (`upd;t;x);i+:1];}];

\d .

.tp.init:{
  .event.addhandler[`.z.pc;`.u.pc];
  if[.qi.isproc;
    $[count system"a .";
      .u.tick`;
      '"\ntp expects tables to be defined with a '-schemas' option at the cmd line e.g. \n   ... -schemas alpaca or -schemas binance,kraken\n"]];
  }