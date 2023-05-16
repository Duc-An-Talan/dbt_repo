{%- macro hoyquiero(flavor, dessert = "ice cream") -%}

Today I want {{ flavor }} {{ dessert }}!

{%- endmacro -%}

{{ hoyquiero(flavor = "chocolate") }}


{{ hoyquiero("mango", "sorbet") }}

