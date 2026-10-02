-- Category list from contract/categories.md (contract v1).
-- A new category gets its own migration, added before branches start sending it.
insert into category (category_code, description) values
    ('BEVERAGE',      'Drinks: soft drinks, water, coffee, tea, juice'),
    ('SNACK',         'Packaged snacks, confectionery'),
    ('READY_MEAL',    'Ready-to-eat and heat-and-eat meals'),
    ('FRESH_FOOD',    'Bakery, fruit, dairy and other short-shelf-life food'),
    ('HOUSEHOLD',     'Cleaning supplies and household goods'),
    ('PERSONAL_CARE', 'Toiletries and personal care products'),
    ('OTHER',         'Anything not covered above');
