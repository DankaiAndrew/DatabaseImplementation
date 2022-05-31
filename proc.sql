/* (1)  Each shop should sell at least one product. */
/* Behavior: a new shop entity can only be inserted in transaction together with inserting sells. */
/*

Triggers

*/
create or replace function insert_shop_func()
returns trigger as $$
begin
    if not exists (select 1 from sells where sells.shop_id = new.id) then
        delete from shop where shop.id = new.id;
        raise exception 'Each shop should sell at least one product!';
    end if;
    return new;
end;
$$ language plpgsql;

create constraint trigger insert_shop
after insert on shop
deferrable initially deferred
for each row execute function insert_shop_func();

/* (2)  An order must involve one or more products from one or more shops.  */
/* Behavior: a new order entity can only be inserted in transaction together with inserting orderline. */

create or replace function insert_orders_func()
returns trigger as $$
begin
    if not exists (select 1 from orderline where orderline.order_id = new.id) then
        delete from orders where orders.id = new.id;
        raise exception 'An order must involve one or more products from one or more shops!';
    end if;
    return new;
end;
$$ language plpgsql;

create constraint trigger insert_orders
after insert on orders
deferrable initially deferred
for each row execute function insert_orders_func();

/*（3）A coupon can only be used on an order whose total amount (before the coupon is applied) exceeds 
the minimum order amount. */

create or replace function insert_orders_coupon_func()
returns trigger as $$
begin
    if new.coupon_id = null or new.payment_amount <= (select min_order_amount from coupon_batch where coupon_batch.id = new.coupon_id) then
        new.coupon_id = null;
    else 
        new.payment_amount = new.payment_amount - (select reward_amount from coupon_batch where coupon_batch.id = new.coupon_id);
    end if;
    return new; -- prob
end;
$$ language plpgsql;

create trigger insert_orders_coupon
before insert on orders
for each row execute function insert_orders_coupon_func();

/* (4)  The refund quantity must not exceed the ordered quantity. */

create or replace function refund_quantity_constraint_func()
returns trigger as $$
begin
    if new.quantity > (select orderline.quantity
                        from orderline 
                        where orderline.order_id = new.order_id 
                        and orderline.shop_id = new.shop_id
                        and orderline.product_id = new.product_id
                        and orderline.sell_timestamp = new.sell_timestamp) then
        return null;
    else
        return new;
    end if;
end;
$$ language plpgsql;

create trigger refund_quantity_constraint
before insert on refund_request
for each row execute function refund_quantity_constraint_func();

/* (5)  The refund request date must be within 30 days of the delivery date. */

create or replace function refund_date_constraint_func()
returns trigger as $$
begin
    if new.request_date > (select coalesce(orderline.delivery_date, '-infinity') + interval '30 day'
                        from orderline 
                        where orderline.order_id = new.order_id 
                        and orderline.shop_id = new.shop_id
                        and orderline.product_id = new.product_id
                        and orderline.sell_timestamp = new.sell_timestamp) then
        return null;
    else
        return new;
    end if;
end;
$$ language plpgsql;

create trigger refund_date_constraint
before insert on refund_request
for each row execute function refund_date_constraint_func();

/* (6)  Refund request can only be made for a delivered product. */

create or replace function refund_status_constraint_func()
returns trigger as $$
begin
    if not exists (select 1
                        from orderline 
                        where orderline.order_id = new.order_id 
                        and orderline.shop_id = new.shop_id
                        and orderline.product_id = new.product_id
                        and orderline.sell_timestamp = new.sell_timestamp
                        and orderline.status = 'delivered') then
        return null;
    else
        return new;
    end if;
end;
$$ language plpgsql;

create trigger refund_status_constraint
before insert on refund_request
for each row execute function refund_status_constraint_func();

/* (7)  A user can only make a product review for a product that they themselves purchased. */

create or replace function review_user_constraint_func()
returns trigger as $$
begin
    if (select comment.user_id from comment where comment.id = new.id) = (
        select orders.user_id from orders where orders.id = new.order_id
    ) then
        return new;
    else
        return null;
    end if;
end;
$$ language plpgsql;

create trigger review_user_constraint
before insert on review
for each row execute function review_user_constraint_func();

/* (8)  A comment is either a review or a reply, not both (non-overlapping and covering).  */
create or replace function review_constraint_func()
returns trigger as $$
begin
    if new.id not in (select reply.id from reply) then
        return new;
    else
        return null;
    end if;
end;
$$ language plpgsql;

create trigger review_constraint
before insert on review
for each row execute function review_constraint_func();

create or replace function reply_constraint_func()
returns trigger as $$
begin
    if new.id not in (select review.id from review) then
        return new;
    else
        return null;
    end if;
end;
$$ language plpgsql;

create trigger reply_constraint
before insert on reply
for each row execute function reply_constraint_func();

/* (9)  A reply has at least one reply version. */

create or replace function reply_version_constraint_func()
returns trigger as $$
begin
    if not exists (select 1 from reply_version where reply_version.reply_id = new.id) then
        delete from reply where reply.id = new.id;
        raise exception 'A reply needs to have at least one reply version!';
    end if;
    return new;
end;
$$ language plpgsql;

create constraint trigger reply_version_constraint
after insert on reply
deferrable initially deferred
for each row execute function reply_version_constraint_func();

/* (10) A review has at least one review version. */

create or replace function review_version_constraint_func()
returns trigger as $$
begin
    if not exists (select 1 from review_version where review_version.review_id = new.id) then --prob
        delete from review where review.id = new.id;
        raise exception 'A review needs to have at least one review version!';
    end if;
    return new;
end;
$$ language plpgsql;

create constraint trigger review_version_constraint
after insert on review
deferrable initially deferred
for each row execute function review_version_constraint_func();

/* (11) A delivery complaint can only be made when the product has been delivered. */

create or replace function delivery_complaint_constraint_func()
returns trigger as $$
begin
    if not exists (select 1
                        from orderline 
                        where orderline.order_id = new.order_id 
                        and orderline.shop_id = new.shop_id
                        and orderline.product_id = new.product_id
                        and orderline.sell_timestamp = new.sell_timestamp
                        and orderline.status = 'delivered') then
        return null;
    else
        return new;
    end if;
end;
$$ language plpgsql;

create trigger delivery_complaint_constraint
before insert on delivery_complaint
for each row execute function delivery_complaint_constraint_func();

/* (12) A complaint is either a delivery-related complaint, a shop-related complaint or a comment-related 
complaint (non-overlapping and covering) */

create or replace function exlucsive_shop_complaint_constraint_func()
returns trigger as $$
begin
    if new.id not in (select comment_complaint.id from comment_complaint) and
        new.id not in (select delivery_complaint.id from delivery_complaint) then
        return new;
    else
        return null;
    end if;
end;
$$ language plpgsql;

create trigger exlucsive_shop_complaint_constraint
before insert on shop_complaint
for each row execute function exlucsive_shop_complaint_constraint_func();

create or replace function exlucsive_comment_complaint_constraint_func()
returns trigger as $$
begin
    if new.id not in (select shop_complaint.id from shop_complaint) and
        new.id not in (select delivery_complaint.id from delivery_complaint) then
        return new;
    else
        return null;
    end if;
end;
$$ language plpgsql;

create trigger exlucsive_comment_complaint_constraint
before insert on comment_complaint
for each row execute function exlucsive_comment_complaint_constraint_func();

create or replace function exlucsive_delivery_complaint_constraint_func()
returns trigger as $$
begin
    if new.id not in (select shop_complaint.id from shop_complaint) and
        new.id not in (select comment_complaint.id from comment_complaint) then
        return new;
    else
        return null;
    end if;
end;
$$ language plpgsql;

create trigger exlucsive_delivery_complaint_constraint
before insert on delivery_complaint
for each row execute function exlucsive_delivery_complaint_constraint_func();






/*
Procedures
*/
-- Procedure 1
CREATE OR REPLACE PROCEDURE place_order(
    user_id INTEGER, coupon_id INTEGER, 
    shipping_address TEXT, shop_ids INTEGER[], 
    product_ids INTEGER[], sell_timestamps TIMESTAMP[], 
    quantities INTEGER[], shipping_costs NUMERIC[]
)
AS $$
DECLARE
num_item NUMERIC;
cid INTEGER;
order_id INTEGER;
cost NUMERIC;
temp NUMERIC;
i INTEGER;
BEGIN
    cost := 0;
    temp := 0;
    SELECT array_length(shop_ids, 1) INTO num_item;
    IF EXISTS(
        SELECT 1 FROM orders O 
        WHERE O.user_id = $1
        AND O.coupon_id = $2
    ) THEN 
        cid := NULL;
    ELSE
        cid := coupon_id;
    END IF;
    FOR i IN 1..num_item LOOP
        IF quantities[i] > 0 THEN
            UPDATE sells
            SET quantity = quantity - quantities[i]
            WHERE shop_ids[i] = shop_id 
            and product_ids[i] = product_id
            and sell_timestamps[i] = sell_timestamp;
        
            SELECT price FROM sells  
            WHERE shop_ids[i] = shop_id 
            and product_ids[i] = product_id
            and sell_timestamps[i] = sell_timestamp
            INTO temp;
            cost := cost + temp * quantities[i] + shipping_costs[i];
        END IF;
        i := i + 1;
    END LOOP;
    INSERT INTO orders (user_id, coupon_id, shipping_address, payment_amount) VALUES (
        user_id,
        coupon_id,
        shipping_address,
        cost
    ) RETURNING id INTO order_id;
    FOR i IN 1..num_item LOOP
        INSERT INTO orderline (order_id, shop_id, product_id, sell_timestamp, quantity, shipping_cost, status) VALUES (
            order_id,
            shop_ids[i],
            product_ids[i],
            sell_timestamps[i],
            quantities[i],
            shipping_costs[i],
            'being_processed'
        );
    END LOOP;
END;    
$$ LANGUAGE plpgsql;

-- Procedure 2
CREATE OR REPLACE PROCEDURE review(
    user_id INTEGER, order_id INTEGER, shop_id INTEGER, 
    product_id INTEGER, sell_timestamp TIMESTAMP, 
    content TEXT, rating INTEGER, comment_timestamp TIMESTAMP
)
AS $$
DECLARE 
comment_id INTEGER;
BEGIN
    IF NOT EXISTS (
        SELECT (r.order_id , r.shop_id, r.product_id, r.sell_timestamp) FROM review r
        WHERE r.order_id = $2
        AND r.shop_id = $3
        AND r.product_id = $4
        AND r.sell_timestamp = $5
    ) 
    THEN
    INSERT INTO comment(user_id) VALUES (
        user_id
    ) RETURNING id INTO comment_id;
    INSERT INTO review (id, order_id, shop_id, product_id, sell_timestamp) VALUES (
        comment_id,
        order_id,
        shop_id,
        product_id,
        sell_timestamp
    );
    ELSE
    SELECT id FROM review r
        WHERE r.order_id = $2
        AND r.shop_id = $3
        AND r.product_id = $4
        AND r.sell_timestamp = $5
    INTO comment_id; 
    END IF;
    IF EXISTS (SELECT 1 FROM review WHERE id = comment_id) THEN
    INSERT INTO review_version (review_id, review_timestamp, content, rating) VALUES (
        comment_id,
        comment_timestamp,
        content,
        rating
    );
    END IF;

END;
$$ LANGUAGE plpgsql;

-- Procedure 3
CREATE OR REPLACE PROCEDURE reply( 
    user_id INTEGER, other_comment_id INTEGER, 
    content TEXT, reply_timestamp TIMESTAMP 
)
AS $$
DECLARE
comment_id INTEGER;
BEGIN
    IF EXISTS (
        SELECT 1 FROM comment WHERE comment.id = other_comment_id
    ) THEN
    INSERT INTO comment (user_id) VALUES (
        user_id
    ) RETURNING id INTO comment_id;

        INSERT INTO reply (id, other_comment_id) VALUES (
            comment_id,
            other_comment_id
        );
        INSERT INTO reply_version (reply_id, reply_timestamp, content) VALUES (
            comment_id,
            reply_timestamp,
            content
    );
    END IF;
END;
$$ LANGUAGE plpgsql;



/*
Function
*/
CREATE OR REPLACE FUNCTION view_comments(IN shop_id INTEGER,product_id INTEGER,sell_timestamp TIMESTAMP)
RETURNS TABLE(username TEXT, content TEXT, rating INTEGER ,comment_timestamp TIMESTAMP) AS $$
DECLARE
shop_id_input INTEGER;
product_id_input INTEGER;
sell_timestamp_input TIMESTAMP;
BEGIN
    shop_id_input := shop_id;
    product_id_input := product_id;
    sell_timestamp_input := sell_timestamp;

    RETURN QUERY

    with RECURSIVE selected_comment as(
        select v.id,'review' as type
        from review v
        where v.shop_id = shop_id_input and v.product_id = product_id_input and v.sell_timestamp = sell_timestamp_input

        UNION ALL

        select r.id,'reply' as type
        from reply r,selected_comment s
        where r.other_comment_id = s.id
        ),

    review_table as(
        select s.id,review.review_timestamp as timestamp_output,review.content as content_output,review.rating as rating_output
        from selected_comment s,review_version review 
        where s.id = review.review_id and review.review_timestamp >= all(
            select r2.review_timestamp from review_version r2 where r2.review_id = s.id)
        ),

    reply_table as(
        select s.id,reply.reply_timestamp as timestamp_output,reply.content as content_output,cast(null as INTEGER) as rating_output
        from selected_comment s,reply_version reply 
        where s.id = reply.reply_id and reply.reply_timestamp >= all(
            select r2.reply_timestamp from reply_version r2 where r2.reply_id = s.id)
        ),

    final_table as (
        select id,timestamp_output,content_output,rating_output from review_table
        union all
        select id,timestamp_output,content_output,rating_output from reply_table
        )

    
    select 
    CASE 
        WHEN u.account_closed = TRUE THEN 'A Deleted User'
        ELSE u.name
    END as usename,final.content_output as content,final.rating_output as rating,final.timestamp_output as comment_timestamp
    from final_table final,users u
    where final.id = u.id
    order by final.timestamp_output asc,final.id asc;

END;
$$ LANGUAGE plpgsql;




CREATE OR REPLACE FUNCTION  get_most_returned_products(IN manufacturer_id INTEGER,n INTEGER)
RETURNS TABLE (product_id INTEGER, product_name TEXT, return_rate NUMERIC(3, 2)) AS $$
DECLARE
BEGIN
    RETURN QUERY
    with manufacturer_return as(
        SELECT distinct refund.product_id as id,sum(refund.quantity) as return_count
        from refund_request refund left join product prod on refund.product_id = prod.id
        where prod.manufacturer = manufacturer_id and refund.status = 'accepted'
        group by refund.product_id
        ),

    manufacturer_delivered as(
        SELECT distinct prod.id as id,sum(orderli.quantity) as deliver_count
        from product prod left join orderline orderli on prod.id = orderli.product_id
        where prod.manufacturer = manufacturer_id and orderli.status = 'delivered'
        group by prod.id
        ),

    manufacturer_product as (
        Select  distinct prod.id
        from product prod
        where prod.manufacturer = manufacturer_id
        ),

    final_table as (
        select m1.id ,p1.name, 
        coalesce(CAST(m2.deliver_count AS INTEGER),0) as deliver_count,coalesce(CAST(m3.return_count AS INTEGER),0) as return_count
        from manufacturer_product as m1
        left join
        manufacturer_delivered as m2 on m1.id = m2.id
        left join manufacturer_return as m3 on m2.id = m3.id,product p1
        where m1.id = p1.id
        )

    select final.id ,final.name as product_name, 
    case 
    when final.deliver_count = 0 then 0.00
    else round(CAST(final.return_count AS NUMERIC) / CAST(final.deliver_count AS NUMERIC) ,2)
    end as rate 
    from final_table final
    order by rate DESC,final.id
    limit n;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION get_worst_shops(IN n INTEGER)
RETURNS TABLE ( shop_id     INTEGER,
                shop_name   TEXT,
                num_negative_indicators INTEGER) 
AS $$

BEGIN

    RETURN QUERY
    WITH D_c_num as (
        SELECT      sth_1.shop_id as id, count(*) as d_c_num
        FROM        (   SELECT DISTINCT d1.order_id, d1.shop_id, d1.product_id, d1.sell_timestamp
                        FROM            delivery_complaint d1   ) as sth_1
        GROUP BY    sth_1.shop_id
    ),

    Shop_c_num as (
        SELECT      s.shop_id as id, count(*) as shop_c_num
        FROM        shop_complaint s
        GROUP BY    s.shop_id
    ),

    Refund_num as (
        SELECT      sth3.shop_id as id, count(*) as refund_num
        FROM        (   SELECT DISTINCT r.order_id, r.shop_id, r.product_id, r.sell_timestamp
                        FROM    refund_request r    ) as sth3
        GROUP BY    sth3.shop_id
    ),

    One_star_num as (
        SELECT      R.shop_id as id, count(*) as one_star_num
        FROM        review  R, review_version review
        WHERE       R.id = review.review_id
        AND         review.review_timestamp = (SELECT   max(R1.review_timestamp)
                                        FROM    review_version R1
                                        WHERE   R1.review_id = R.id)
        AND         review.rating = 1
        GROUP BY    R.shop_id
    )

    SELECT      id, name, cast((coalesce(d_c_num,0)+coalesce(shop_c_num,0) + coalesce(refund_num,0)+coalesce(one_star_num,0)) as INTEGER) as num_negative_indicators 
    FROM        shop natural left outer join D_c_num 
                    natural left outer join Shop_c_num 
                    natural left outer join Refund_num 
                    natural left outer join One_star_num
    ORDER BY    num_negative_indicators DESC,id ASC
    LIMIT       n;

END;
$$ LANGUAGE plpgsql;