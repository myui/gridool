select 
	*
from
	( select count(1) as C1 from lineitem ) as L,
    ( select count(1) as C2 from orders ) as O,
	( select count(1) as C3 from partsupp ) as PS,
    ( select count(1) as C4 from part ) as P,
    ( select count(1) as C5 from customer ) as C,
    ( select count(1) as C6 from supplier ) as S,
    ( select count(1) as C7 from nation ) as N,
    ( select count(1) as C8 from region ) as R;