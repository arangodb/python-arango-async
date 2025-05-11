.. _Serialization:

Serialization
-------------

There are two serialization mechanisms employed by the driver:

* JSON serialization/deserialization
* Document serialization/deserialization

All serializers must inherit from the :class:`arangoasync.serialization.Serializer` class. They must
implement a :func:`arangoasync.serialization.Serializer.dumps` method can handle both
single objects and sequences.

Deserializers mush inherit from the :class:`arangoasync.serialization.Deserializer` class. These have
two methods, :func:`arangoasync.serialization.Deserializer.loads` and :func:`arangoasync.serialization.Deserializer.loads_many`,
which must handle loading of a single document and multiple documents, respectively.

JSON
====

Usually there's no need to implement your own JSON serializer/deserializer, but such an
implementation could look like the following.

**Example:**

.. code-block:: python

    import json
    from typing import Sequence, cast
    from arangoasync.collection import StandardCollection
    from arangoasync.database import StandardDatabase
    from arangoasync.exceptions import DeserializationError, SerializationError
    from arangoasync.serialization import Serializer, Deserializer
    from arangoasync.typings import Json, Jsons


    class CustomJsonSerializer(Serializer[Json]):
        def dumps(self, data: Json | Sequence[str | Json]) -> str:
            try:
                return json.dumps(data, separators=(",", ":"))
            except Exception as e:
                raise SerializationError("Failed to serialize data to JSON.") from e


    class CustomJsonDeserializer(Deserializer[Json, Jsons]):
        def loads(self, data: bytes) -> Json:
            try:
                return json.loads(data)  # type: ignore[no-any-return]
            except Exception as e:
                raise DeserializationError("Failed to deserialize data from JSON.") from e

        def loads_many(self, data: bytes) -> Jsons:
            return self.loads(data)  # type: ignore[return-value]

You would then use the custom serializer/deserializer when creating a client:

.. code-block:: python

    from arangoasync import ArangoClient
    from arangoasync.auth import Auth

    # Initialize the client for ArangoDB.
    async with ArangoClient(
        hosts="http://localhost:8529",
        serializer=CustomJsonSerializer(),
        deserializer=CustomJsonDeserializer(),
    ) as client:
        auth = Auth(username="root", password="passwd")

        # Connect to "test" database as root user.
        test = await client.db("test", auth=auth)

Documents
=========

By default, the JSON serializer/deserializer is used for documents too, but you can provide your own
document serializer and deserializer for fine-grained control over the format of a collection. Say
that you are modeling your students data using Pydantic_. You want to be able to insert documents
of a certain type, and also be able to read them back. More so, you would like to get multiple documents
back using one of the formats provided by pandas_.

**Example:**

.. code-block:: python

    import json
    import pandas as pd
    import pydantic
    import pydantic_core
    from typing import Sequence, cast
    from arangoasync import ArangoClient
    from arangoasync.auth import Auth
    from arangoasync.collection import StandardCollection
    from arangoasync.database import StandardDatabase
    from arangoasync.exceptions import DeserializationError, SerializationError
    from arangoasync.serialization import Serializer, Deserializer
    from arangoasync.typings import Json, Jsons


    class Student(pydantic.BaseModel):
        name: str
        age: int


    class StudentSerializer(Serializer[Student]):
        def dumps(self, data: Student | Sequence[Student | str]) -> str:
            try:
                if isinstance(data, Student):
                    return data.model_dump_json()
                else:
                    # You are required to support both str and Student types.
                    serialized_data = []
                    for student in data:
                        if isinstance(student, str):
                            serialized_data.append(student)
                        else:
                            serialized_data.append(student.model_dump())
                    return json.dumps(serialized_data, separators=(",", ":"))
            except Exception as e:
                raise SerializationError("Failed to serialize data.") from e


    class StudentDeserializer(Deserializer[Student, pd.DataFrame]):
        def loads(self, data: bytes) -> Student:
            # Load a single document.
            try:
                return Student.model_validate(pydantic_core.from_json(data))
            except Exception as e:
                raise DeserializationError("Failed to deserialize data.") from e

        def loads_many(self, data: bytes) -> pd.DataFrame:
            # Load multiple documents.
            return pd.DataFrame(json.loads(data))

You would then use the custom serializer/deserializer when working with collections.

**Example:**

..  code-block:: python

    async def main():
        # Initialize the client for ArangoDB.
        async with ArangoClient(
                hosts="http://localhost:8529",
                serializer=CustomJsonSerializer(),
                deserializer=CustomJsonDeserializer(),
        ) as client:
            auth = Auth(username="root", password="passwd")

            # Connect to "test" database as root user.
            db: StandardDatabase = await client.db("test", auth=auth, verify=True)

            # Populate the "students" collection.
            col = cast(
                StandardCollection[Student, Student, pd.DataFrame],
                db.collection(
                    "students",
                    doc_serializer=StudentSerializer(),
                    doc_deserializer=StudentDeserializer()),
            )

            # Insert one document.
            doc = cast(Json, await col.insert(Student(name="John Doe", age=20)))

            # Insert multiple documents.
            docs = cast(Jsons, await col.insert_many([
                Student(name="Jane Doe", age=22),
                Student(name="Alice Smith", age=19),
                Student(name="Bob Johnson", age=21),
            ]))

            # Get one document.
            john = await col.get(doc)
            assert type(john) == Student

            # Get multiple documents.
            keys = [doc["_key"] for doc in docs]
            students = await col.get_many(keys)
            assert type(students) == pd.DataFrame

.. _Pydantic: https://docs.pydantic.dev/latest/
.. _pandas: https://pandas.pydata.org/
